#  Suzani Yarns Notification Service

A lightweight, asynchronous notification service designed for high-volume transactional emails and SMS. Built with **Python 3.7+** and `asyncio`, this demo simulates a real-world e-commerce notification pipeline with idempotency checks, retry logic, and channel-specific routing.

## ✨ Features

*   **Async Processing:** Non-blocking queue management using `asyncio`.
*   **Multi-Channel Support:** Handles both Email (SendGrid) and SMS (Africa's Talking) simulations.
*   **Smart Routing:** Automatically triggers SMS confirmations for high-value orders (> KES 5,000).
*   **Idempotency:** Prevents duplicate notifications using unique keys.
*   **Resilience:** Implements exponential backoff for failed delivery attempts.
*   **Zero Dependencies:** Runs on standard library modules only.

## 🚀 Quick Start

1. **Clone the repository:**
   ```bash
   git clone https://github.com/ToniMoringa/suzani-yarns-notifications.git
   cd suzani-yarns-notifications

   ## Run the demo:
   ```
      python main.py
   ```

  **Architecture**
   
   The service uses a producer-consumer pattern:
   
## Producer: Enqueues order confirmations based on business logic (e.g., high-value thresholds).
## Queue: An in-memory asyncio.Queue holds pending notifications.
## Workers: Concurrent tasks process the queue, handling retries and status updates.

## Scaling Strategy

 To move this from a demo to a production environment:

| Component | Current (Demo) | Production Recommendation |
|-----------|----------------|---------------------------|
| Queue | `asyncio.Queue` (RAM) | Redis or RabbitMQ for persistence |
| Workers | Local Python Tasks | Celery or BullMQ for distributed processing |
| Storage | Python Dictionary | PostgreSQL for audit logs and status tracking |
| API | Direct Function Call | FastAPI or Express for HTTP endpoints |

**Tech Stack**
Language: Python 3.7+

Concurrency: asyncio

Data Structures: dataclasses, enum, collections
 
