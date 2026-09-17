// Package http provides HTTP handlers and routing for the REST API.
//
// Роль пакета в системе:
// Принимает входящие сетевые HTTP-запросы клиентов (REST API), валидирует параметры,
// делегирует выполнение LocationService и HealthHandler, и формирует JSON-ответы.
package http

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"courier-service/internal/domain"
	"courier-service/internal/service"
)

// LocationHandler обрабатывает клиентские запросы получения геопозиции курьера.
// Зависит от LocationService (слой бизнес-логики).
type LocationHandler struct {
	locationService service.LocationService
}

// NewLocationHandler создает экземпляр LocationHandler с внедренным LocationService.
func NewLocationHandler(locationService service.LocationService) *LocationHandler {
	return &LocationHandler{
		locationService: locationService,
	}
}

// GetCourierLocation обрабатывает эндпоинт GET /orders/{orderId}/courier-location.
//
// Строго соблюдает SLA < 100 мс:
// 1. Быстрый путь (Cache Hit): координаты уже в Redis.
//    Возвращает HTTP 200 OK + JSON (~1.6 мкс).
// 2. Холодный старт (Cache Miss): данных в Redis еще нет.
//    Не ждет 700 мс синхронных апстримов! Мгновенно возвращает HTTP 202 Accepted
//    с заголовком Retry-After: 1, инициируя асинхронный фоновый прогрев в воркере.
// 3. Некорректный ID заказа: возвращает HTTP 400 Bad Request.
func (h *LocationHandler) GetCourierLocation(w http.ResponseWriter, r *http.Request) {
	// 1. Извлечение и валидация параметра пути orderId (Go 1.22+ PathValue)
	orderIDStr := r.PathValue("orderId")
	orderID, err := strconv.ParseInt(orderIDStr, 10, 64)
	if err != nil || orderID <= 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "invalid order id, must be a positive integer",
		})
		return
	}

	// 2. Обращение к сервису чтения O(1)
	location, found, err := h.locationService.GetCourierLocation(r.Context(), orderID)
	if err != nil {
		slog.Error("Failed to get courier location",
			slog.Int64("order_id", orderID),
			slog.String("error", err.Error()),
		)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "internal server error",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")

	// 3. Ветвление: Cache Miss -> 202 Accepted (неблокирующий ответ)
	if !found {
		// Клиенту предлагается повторить запрос через 1 секунду, когда воркер согреет кэш
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(domain.TrackingResponse{
			Status:  "pending",
			Message: "Location tracking started, coordinates will be available shortly",
		})
		return
	}

	// 4. Ветвление: Cache Hit -> 200 OK (координаты курьера готовы)
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(location)
}
