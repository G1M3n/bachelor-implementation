import uuid
from datetime import datetime
from sqlalchemy import (
    Column,
    String,
    ForeignKey,
    Date,
    Boolean,
    Integer,
    DECIMAL,
    TIMESTAMP,
    Time,
    DateTime,
    Enum,
)
from sqlalchemy.orm import relationship
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.types import TypeDecorator, CHAR

from common.schemas import Gender


class GUID(TypeDecorator):
    impl = CHAR
    cache_ok = True

    def load_dialect_impl(self, dialect):
        return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        if not isinstance(value, uuid.UUID):
            return str(uuid.UUID(value))
        else:
            return str(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        return uuid.UUID(value)


class Base(AsyncAttrs, DeclarativeBase):
    pass


class EventParticipant(Base):
    __tablename__ = "event_participants"

    event_id = Column(
        GUID(), ForeignKey("event.event_id", ondelete="CASCADE"), primary_key=True
    )
    user_id = Column(
        GUID(), ForeignKey("users.user_id", ondelete="CASCADE"), primary_key=True
    )
    added_at = Column(DateTime, default=datetime.utcnow)

    event = relationship("Event", back_populates="participants", lazy="selectin")
    user = relationship("User", back_populates="events", lazy="selectin")


class RefreshToken(Base):
    __tablename__ = "refresh_tokens"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    token = Column(String(255), nullable=False, unique=True)
    sub = Column(String(255), nullable=False)
    user_id = Column(GUID(), nullable=False)
    role = Column(String(100), nullable=False)
    expires_at = Column(DateTime, nullable=False)
    revoked = Column(Boolean, default=False)


class PasswordResetToken(Base):
    __tablename__ = "password_reset_token"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), nullable=False, unique=True)
    token = Column(String(255), unique=True, nullable=False)
    expires_at = Column(DateTime, nullable=False)


class RegistrationToken(Base):
    __tablename__ = "registration_tokens"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    sub = Column(String(512), nullable=False, unique=True)
    token = Column(String(255), unique=True, nullable=False)
    expires_at = Column(DateTime, nullable=False)


class Club(Base):
    __tablename__ = "club"

    club_id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    club_name = Column(String(255), nullable=False)

    users = relationship("User", back_populates="club", lazy="selectin")


class User(Base):
    __tablename__ = "users"

    user_id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    username = Column(String(100), nullable=True, unique=True, index=True)
    first_name = Column(String(100))
    last_name = Column(String(100))
    gender = Column(Enum(Gender), nullable=False, default=Gender.UNKNOWN)
    email = Column(String(255), unique=True, nullable=True)
    birthday = Column(Date)
    hashed_password = Column(String(255), nullable=False)
    role = Column(String(100), default="user")
    notify = Column(Boolean, default=False)
    club_id = Column(GUID(), ForeignKey("club.club_id", ondelete="SET NULL"))

    club = relationship("Club", back_populates="users", lazy="selectin")
    transponder_assignments = relationship(
        "TransponderAssignment", back_populates="user", lazy="selectin"
    )
    events = relationship(
        "EventParticipant",
        back_populates="user",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    trackings = relationship("Tracking", back_populates="user", lazy="selectin")


class Transponder(Base):
    __tablename__ = "transponder"

    transponder_id = Column(String(255), nullable=False, primary_key=True, unique=True)

    assignments = relationship(
        "TransponderAssignment", back_populates="transponder", lazy="selectin"
    )


class TransponderAssignment(Base):
    __tablename__ = "transponder_assignment"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    user_id = Column(GUID(), ForeignKey("users.user_id", ondelete="CASCADE"))
    transponder_id = Column(
        String(255), ForeignKey("transponder.transponder_id", ondelete="CASCADE")
    )
    assign_start = Column(DateTime, nullable=False)
    assign_end = Column(DateTime, nullable=False)

    user = relationship(
        "User", back_populates="transponder_assignments", lazy="selectin"
    )
    transponder = relationship(
        "Transponder", back_populates="assignments", lazy="selectin"
    )


class Track(Base):
    __tablename__ = "track"

    track_id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    distanz = Column(DECIMAL(10, 2))
    activ = Column(Boolean, default=False)

    track_tracking_point = relationship(
        "TrackTrackingPoint", back_populates="track", lazy="selectin"
    )
    trackings = relationship("Tracking", back_populates="track", lazy="selectin")


class TrackingPoint(Base):
    __tablename__ = "tracking_point"
    tracking_point_id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    device_id = Column(String(255), nullable=False, unique=True)
    name = Column(String(255), nullable=False)
    distance = Column(DECIMAL(10, 2), nullable=False)
    loop_id = Column(String(255), nullable=False)
    pointtype = Column(Integer, nullable=False)
    min_split_time = Column(Time)
    max_split_time = Column(Time)
    least_time = Column(
        Time, default=lambda: datetime.strptime("00:00:00", "%H:%M:%S").time()
    )
    latitude = Column(DECIMAL(10, 7))
    longitude = Column(DECIMAL(10, 7))
    activ = Column(Boolean, default=True)
    track_tracking_point = relationship(
        "TrackTrackingPoint", back_populates="tracking_point", lazy="selectin"
    )


class TrackTrackingPoint(Base):
    __tablename__ = "track_tracking_point"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    track_id = Column(GUID(), ForeignKey("track.track_id", ondelete="CASCADE"))
    tracking_point_id = Column(
        String(255), ForeignKey("tracking_point.tracking_point_id", ondelete="CASCADE")
    )
    track = relationship(
        "Track", back_populates="track_tracking_point", lazy="selectin"
    )
    tracking_point = relationship(
        "TrackingPoint", back_populates="track_tracking_point", lazy="selectin"
    )


class Tracking(Base):
    __tablename__ = "tracking"

    tracking_id = Column(GUID(), primary_key=True, unique=True, default=uuid.uuid4)
    start_date_time = Column(TIMESTAMP(timezone=True))
    time = Column(Time)
    user_id = Column(GUID(), ForeignKey("users.user_id", ondelete="SET NULL"))
    track_id = Column(GUID(), ForeignKey("track.track_id", ondelete="RESTRICT"))
    event_id = Column(GUID(), ForeignKey("event.event_id", ondelete="SET NULL"))
    user = relationship("User", back_populates="trackings", lazy="selectin")
    track = relationship("Track", back_populates="trackings", lazy="selectin")
    event = relationship("Event", back_populates="trackings", lazy="selectin")
    measurements = relationship(
        "TrackingMeasurement",
        back_populates="tracking",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class TrackingMeasurement(Base):
    __tablename__ = "tracking_measurement"
    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    tracking_id = Column(GUID(), ForeignKey("tracking.tracking_id", ondelete="CASCADE"))
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)
    distanz = Column(DECIMAL(10, 2))
    name = Column(String(255), nullable=False)
    tracking = relationship("Tracking", back_populates="measurements", lazy="selectin")


class Event(Base):
    __tablename__ = "event"

    event_id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    start = Column(TIMESTAMP(timezone=True))
    end = Column(TIMESTAMP(timezone=True))
    trackings = relationship("Tracking", back_populates="event", lazy="selectin")
    participants = relationship(
        "EventParticipant",
        back_populates="event",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
