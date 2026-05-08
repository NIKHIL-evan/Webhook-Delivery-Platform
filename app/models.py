# Defines database table models and Pydantic request/response models.
# One database model per table: Endpoint, Event, DeliveryAttempt.
# One Pydantic model per API request/response shape.
import uuid
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import DateTime
from typing import Optional
from datetime import timezone
from datetime import datetime
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

class Base(DeclarativeBase):
    pass

class Endpoints(Base):
    __tablename__ = "endpoints"

    url_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    url: Mapped[str]
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class Events(Base):
    __tablename__ = "events"

    url_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("endpoints.url_id"))
    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    status: Mapped[str] = mapped_column(default="pending")
    payload: Mapped[dict] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class DeliveryAttempts(Base):
    __tablename__ = "delivery_attempts"

    event_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("events.event_id"))
    attempt_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    attempt_number: Mapped[int] 
    status: Mapped[str]
    response_code: Mapped[Optional[int]]
    attempted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))