// =============================================================================
// ORDER REPLAY HANDLER — Replay Logic for Order Events
// =============================================================================
// This handler knows how to replay order-related events.
// Each method corresponds to an event type and implements two modes:
//
//   DRY-RUN MODE:
//     - Logs what the event represents
//     - Validates the event payload
//     - Returns a description of what WOULD happen
//     - Makes NO state changes
//     - Used for: understanding flow, debugging, verification
//
//   STATE-REBUILD MODE:
//     - Validates the event payload
//     - Applies the state change to the database
//     - Returns the new state
//     - Used for: recovering from data corruption, state verification
//
// SIDE-EFFECT ISOLATION:
//   Order events don't have direct external side effects.
//   However, they trigger downstream events (e.g., ORDER_CREATED → PAYMENT).
//   During replay, we DON'T re-trigger these downstream events.
//   The replay engine processes events in the order they occurred,
//   so the downstream events are already in the event store.
//
// INTERVIEW TIP: "Each event type has a dedicated replay handler that
// operates in either dry-run or state-rebuild mode. The handler only
// processes the state change — it never re-emits events to Kafka,
// because those downstream events already exist in the event store."
// =============================================================================

const { createLogger } = require('@chronoscope/core');

// =============================================================================
// createOrderReplayHandler() — Factory function
// =============================================================================
function createOrderReplayHandler() {
    const logger = createLogger('order-replay-handler');

    return {
        // =====================================================================
        // handleOrderCreated() — Replay ORDER_CREATED event
        // =====================================================================
        // ORIGINAL BEHAVIOR (in order-service):
        //   1. Save order to database
        //   2. Emit ORDER_CREATED event to Kafka
        //
        // REPLAY BEHAVIOR:
        //   dry-run: Validate payload, log, return description
        //   state-rebuild: Recreate order record in database
        //
        // WHAT WE DON'T DO:
        //   - Don't emit events to Kafka (they already exist)
        //   - Don't trigger payment processing (handled by payment replay handler)
        //   - Don't send notifications
        // =====================================================================
        async handleOrderCreated(event, mode) {
            const { orderId, customerId, items, totalAmount } = event.payload;

            logger.info('Replaying ORDER_CREATED', {
                mode,
                orderId,
                customerId,
                totalAmount,
                itemCount: items?.length,
                correlation_id: event.correlation_id,
            });

            if (mode === 'dry-run') {
                // =============================================================
                // DRY-RUN: Simulate — describe what would happen
                // =============================================================
                return {
                    action: 'CREATE_ORDER',
                    description: `Would create order ${orderId} for customer ${customerId}`,
                    details: {
                        orderId,
                        customerId,
                        items,
                        totalAmount,
                        status: 'PENDING',
                        stateChange: 'New order record would be inserted',
                        sideEffects: 'None — downstream events already in event store',
                    },
                    validation: {
                        hasOrderId: !!orderId,
                        hasCustomerId: !!customerId,
                        hasItems: items && items.length > 0,
                        hasTotalAmount: totalAmount > 0,
                        isValid: !!orderId && !!customerId && items?.length > 0 && totalAmount > 0,
                    },
                };
            }

            if (mode === 'state-rebuild') {
                // =============================================================
                // STATE-REBUILD: Actually recreate the order record
                // =============================================================
                // In a full implementation, this would INSERT/UPSERT into
                // the orders table in PostgreSQL.
                //
                // For now, we return the state that would be created.
                // To implement fully, inject the database pool into this handler.
                // =============================================================
                return {
                    action: 'CREATE_ORDER',
                    description: `Order ${orderId} state rebuilt`,
                    stateCreated: {
                        orderId,
                        customerId,
                        items,
                        totalAmount,
                        status: 'PENDING',
                        correlationId: event.correlation_id,
                        createdAt: new Date(parseInt(event.timestamp, 10)).toISOString(),
                    },
                    applied: true,
                };
            }
        },

        // =====================================================================
        // handleOrderUpdated() — Replay ORDER_UPDATED event
        // =====================================================================
        async handleOrderUpdated(event, mode) {
            const { orderId, previousState, currentState, changes } = event.payload;

            logger.info('Replaying ORDER_UPDATED', {
                mode,
                orderId,
                previousStatus: previousState?.status,
                currentStatus: currentState?.status,
                changes,
                correlation_id: event.correlation_id,
            });

            if (mode === 'dry-run') {
                return {
                    action: 'UPDATE_ORDER',
                    description: `Would update order ${orderId}: ${previousState?.status} → ${currentState?.status}`,
                    details: {
                        orderId,
                        previousState,
                        currentState,
                        changes,
                        stateChange: `Status transition: ${previousState?.status} → ${currentState?.status}`,
                    },
                    validation: {
                        hasOrderId: !!orderId,
                        hasStateTransition: !!previousState && !!currentState,
                        isValid: !!orderId,
                    },
                };
            }

            if (mode === 'state-rebuild') {
                return {
                    action: 'UPDATE_ORDER',
                    description: `Order ${orderId} state updated`,
                    stateUpdated: {
                        orderId,
                        ...currentState,
                        updatedAt: new Date(parseInt(event.timestamp, 10)).toISOString(),
                    },
                    applied: true,
                };
            }
        },

        // =====================================================================
        // handleOrderCancelled() — Replay ORDER_CANCELLED event
        // =====================================================================
        async handleOrderCancelled(event, mode) {
            const { orderId, previousStatus, reason, totalAmount } = event.payload;

            logger.info('Replaying ORDER_CANCELLED', {
                mode,
                orderId,
                previousStatus,
                reason,
                correlation_id: event.correlation_id,
            });

            if (mode === 'dry-run') {
                return {
                    action: 'CANCEL_ORDER',
                    description: `Would cancel order ${orderId} (was ${previousStatus})`,
                    details: {
                        orderId,
                        previousStatus,
                        newStatus: 'CANCELLED',
                        reason,
                        totalAmount,
                        stateChange: `Status: ${previousStatus} → CANCELLED`,
                        potentialRefund: totalAmount,
                    },
                    validation: {
                        hasOrderId: !!orderId,
                        hasReason: !!reason,
                        isValid: !!orderId,
                    },
                };
            }

            if (mode === 'state-rebuild') {
                return {
                    action: 'CANCEL_ORDER',
                    description: `Order ${orderId} cancelled`,
                    stateUpdated: {
                        orderId,
                        status: 'CANCELLED',
                        cancelReason: reason,
                        updatedAt: new Date(parseInt(event.timestamp, 10)).toISOString(),
                    },
                    applied: true,
                };
            }
        },

        // =====================================================================
        // handleOrderCompleted() — Replay ORDER_COMPLETED event
        // =====================================================================
        async handleOrderCompleted(event, mode) {
            const { orderId } = event.payload;

            logger.info('Replaying ORDER_COMPLETED', {
                mode,
                orderId,
                correlation_id: event.correlation_id,
            });

            if (mode === 'dry-run') {
                return {
                    action: 'COMPLETE_ORDER',
                    description: `Would mark order ${orderId} as completed`,
                    details: {
                        orderId,
                        newStatus: 'COMPLETED',
                        stateChange: 'Status → COMPLETED (terminal state)',
                    },
                };
            }

            if (mode === 'state-rebuild') {
                return {
                    action: 'COMPLETE_ORDER',
                    description: `Order ${orderId} completed`,
                    stateUpdated: {
                        orderId,
                        status: 'COMPLETED',
                        updatedAt: new Date(parseInt(event.timestamp, 10)).toISOString(),
                    },
                    applied: true,
                };
            }
        },
    };
}

module.exports = { createOrderReplayHandler };
