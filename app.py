import asyncio
import uuid
import random
import time
from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, Set
from collections import defaultdict

class Channel(Enum):
    EMAIL = "email"
    SMS = "sms"

class Status(Enum):
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"
    DUPLICATE = "duplicate"

@dataclass
class OrderNotification:
    order_id: str
    customer_phone: str
    customer_email: str
    amount_kes: float
    channel: Channel = Channel.EMAIL
    status: Status = Status.PENDING
    attempts: int = 0
    idempotency_key: str = ""
    created_at: float = field(default_factory=time.time)

    def __post_init__(self):
        if not self.idempotency_key:
            self.idempotency_key = f"{self.order_id}-confirmation"

class ProviderAdapter:
    def __init__(self, name: str, failure_rate: float = 0.1):
        self.name = name
        self.failure_rate = failure_rate

    async def send(self, notification: OrderNotification) -> bool:
        await asyncio.sleep(random.uniform(0.1, 0.4))
        if random.random() < self.failure_rate:
            raise ConnectionError(f"{self.name} timeout")
        print(f"  ✅ [{self.name}] Sent {notification.channel.value} for Order #{notification.order_id} "
              f"(KES {notification.amount_kes:,.2f}) to {notification.customer_phone if notification.channel == Channel.SMS else notification.customer_email}")
        return True

class SuzaniNotificationService:
    MAX_RETRIES = 3
    HIGH_VALUE_THRESHOLD_KES = 5000

    def __init__(self):
        self.queue: asyncio.Queue[OrderNotification] = asyncio.Queue()
        self.providers = {
            Channel.EMAIL: ProviderAdapter("SendGrid", failure_rate=0.15),
            Channel.SMS: ProviderAdapter("AfricasTalking", failure_rate=0.1),
        }
        self.sent_keys: Set[str] = set()
        self.store: Dict[str, OrderNotification] = {}
        self._workers = []

    async def enqueue_order_confirmation(self, order_id: str, phone: str, email: str, amount: float):
        notif = OrderNotification(
            order_id=order_id,
            customer_phone=phone,
            customer_email=email,
            amount_kes=amount
        )
        
        if amount >= self.HIGH_VALUE_THRESHOLD_KES:
            sms_notif = OrderNotification(
                order_id=order_id,
                customer_phone=phone,
                customer_email=email,
                amount_kes=amount,
                channel=Channel.SMS,
                idempotency_key=f"{order_id}-sms-confirmation"
            )
            await self.queue.put(sms_notif)
            print(f"📱 High-value order detected: Added SMS confirmation for KES {amount:,.2f}")
        
        await self.queue.put(notif)
        print(f"📥 Enqueued email confirmation for Order #{order_id}")
        return notif.idempotency_key

    async def _worker(self, worker_id: int):
        while True:
            notif = await self.queue.get()
            try:
                if notif.idempotency_key in self.sent_keys:
                    notif.status = Status.DUPLICATE
                    continue

                provider = self.providers[notif.channel]
                await provider.send(notif)
                notif.status = Status.SENT
                self.sent_keys.add(notif.idempotency_key)

            except Exception as e:
                notif.attempts += 1
                if notif.attempts < self.MAX_RETRIES:
                    backoff = 0.1 * (2 ** notif.attempts)
                    print(f"  🔄 Worker-{worker_id}: Retry {notif.attempts}/{self.MAX_RETRIES} after {backoff:.1f}s ({e})")
                    await asyncio.sleep(backoff)
                    await self.queue.put(notif)
                else:
                    notif.status = Status.FAILED
                    print(f"  ❌ Worker-{worker_id}: Failed permanently after {self.MAX_RETRIES} attempts")
            finally:
                self.queue.task_done()
                self.store[notif.idempotency_key] = notif

    async def start(self, num_workers: int = 2):
        for i in range(num_workers):
            self._workers.append(asyncio.create_task(self._worker(i)))
        print(f"🚀 Suzani Yarns Notification Service started ({num_workers} workers)\n")

    async def shutdown(self):
        await self.queue.join()
        for w in self._workers:
            w.cancel()
        print("\n🛑 All order confirmations processed.")

async def main():
    service = SuzaniNotificationService()
    await service.start(num_workers=2)

    test_orders = [
        ("ORD-7821", "+254722123456", "wanjiku@gmail.com", 3500.00),
        ("ORD-7822", "+254711987654", "kofi@yahoo.com", 8200.00),
        ("ORD-7821", "+254722123456", "wanjiku@gmail.com", 3500.00),
        ("ORD-7823", "+254733555666", "amani@outlook.com", 1200.00),
    ]

    for order in test_orders:
        await service.enqueue_order_confirmation(*order)

    await service.shutdown()

    print("\n📊 Order Notification Summary:")
    for key, notif in service.store.items():
        print(f"  {notif.order_id} | {notif.channel.value:5s} | {notif.status.value:10s} | "
              f"KES {notif.amount_kes:>8,.2f} | attempts={notif.attempts}")

if __name__ == "__main__":
    asyncio.run(main())