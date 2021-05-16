# -*- coding: utf-8 -*-
"""
EIDA NG stationlite ORM.
"""

import datetime
import enum

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    Unicode,
)
from sqlalchemy.ext.declarative import declared_attr, declarative_base
from sqlalchemy.orm import relationship

from eidaws.stationlite.core.utils import (
    Epoch as _Epoch,
    RestrictedStatus as _RestrictedStatus,
)


LENGTH_CHANNEL_CODE = 3
LENGTH_DESCRIPTION = 512
LENGTH_LOCATION_CODE = 2
LENGTH_STD_CODE = 32
LENGTH_URL = 256


class Base:
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    id = Column(Integer, primary_key=True)


class CodeMixin(object):
    @declared_attr
    def code(cls):
        return Column(String(LENGTH_STD_CODE), nullable=False, index=True)


class EpochMixin(object):
    @declared_attr
    def starttime(cls):
        return Column(DateTime, nullable=False, index=True)

    @declared_attr
    def endtime(cls):
        return Column(DateTime, index=True)


class LastSeenMixin:
    @declared_attr
    def lastseen(cls):
        return Column(
            DateTime,
            default=datetime.datetime.utcnow,
            onupdate=datetime.datetime.utcnow,
        )


class RestrictedStatusMixin(object):
    @declared_attr
    def restrictedstatus(cls):
        return Column(
            Enum(_RestrictedStatus),
            default=_RestrictedStatus.OPEN,
        )


class DescriptionMixin(object):
    @declared_attr
    def description(cls):
        return Column("description", Unicode(LENGTH_DESCRIPTION))


ORMBase = declarative_base(cls=Base)


class EpochType(ORMBase):
    type = Column(Enum(_Epoch), nullable=False, default=_Epoch.CHANNEL)

    epochs = relationship(
        "Epoch", back_populates="type", cascade="all, delete-orphan"
    )


class Epoch(EpochMixin, RestrictedStatusMixin, LastSeenMixin, ORMBase):
    """
    ORM entity representing a StationXML epoch.
    """

    epochtype_ref = Column(Integer, ForeignKey("epochtype.id"))

    type = relationship("EpochType", back_populates="epochs")
    network_epoch = relationship(
        "NetworkEpoch",
        uselist=False,
        back_populates="epoch",
        cascade="all, delete-orphan",
    )
    station_epoch = relationship(
        "StationEpoch",
        uselist=False,
        back_populates="epoch",
        cascade="all, delete-orphan",
    )
    channel_epoch = relationship(
        "ChannelEpoch",
        uselist=False,
        back_populates="epoch",
        cascade="all, delete-orphan",
    )
    # many to many Epoch<->Endpoint
    endpoints = relationship("Routing", back_populates="epoch")

    def __repr__(self):
        return (
            f"<Epoch(starttime={self.starttime!r}, endtime={self.endtime!r}, "
            f"restricted_status={self.restrictedstatus!r})>"
        )


class Network(CodeMixin, ORMBase):

    network_epochs = relationship(
        "NetworkEpoch", back_populates="network", cascade="all, delete-orphan"
    )
    channel_epochs = relationship(
        "ChannelEpoch", back_populates="network", cascade="all, delete-orphan"
    )
    virtual_channel_epochs = relationship(
        "VirtualChannelEpoch",
        back_populates="network",
        cascade="all, delete-orphan",
    )

    def __repr__(self):
        return f"<Network(code={self.code!r})>"


class NetworkEpoch(DescriptionMixin, ORMBase):

    network_ref = Column(Integer, ForeignKey("network.id"), index=True)
    epoch_ref = Column(Integer, ForeignKey("epoch.id"), index=True)

    network = relationship("Network", back_populates="network_epochs")
    epoch = relationship("Epoch", back_populates="network_epoch")

    def __repr__(self):
        return (
            f"<NetworkEpoch(network={self.network!r}, epoch={self.epoch!r})>"
        )


class Station(CodeMixin, ORMBase):

    station_epochs = relationship(
        "StationEpoch", back_populates="station", cascade="all, delete-orphan"
    )

    channel_epochs = relationship(
        "ChannelEpoch", back_populates="station", cascade="all, delete-orphan"
    )
    virtual_channel_epochs = relationship(
        "VirtualChannelEpoch",
        back_populates="station",
        cascade="all, delete-orphan",
    )

    def __repr__(self):
        return f"<Station(code={self.code!r})>"


class StationEpoch(DescriptionMixin, ORMBase):

    station_ref = Column(Integer, ForeignKey("station.id"), index=True)
    epoch_ref = Column(Integer, ForeignKey("epoch.id"), index=True)
    longitude = Column(Float, nullable=False, index=True)
    latitude = Column(Float, nullable=False, index=True)

    station = relationship("Station", back_populates="station_epochs")
    epoch = relationship("Epoch", back_populates="station_epoch")

    def __repr__(self):
        return (
            f"<StationEpoch(station={self.station!r}, epoch={self.epoch!r})>"
        )


class ChannelEpoch(CodeMixin, ORMBase):

    network_ref = Column(Integer, ForeignKey("network.id"), index=True)
    station_ref = Column(Integer, ForeignKey("station.id"), index=True)
    epoch_ref = Column(Integer, ForeignKey("epoch.id"), index=True)
    locationcode = Column(
        String(LENGTH_LOCATION_CODE), nullable=False, index=True
    )

    network = relationship("Network", back_populates="channel_epochs")
    station = relationship("Station", back_populates="channel_epochs")
    epoch = relationship("Epoch", back_populates="channel_epoch")

    def __repr__(self):
        return (
            f"<ChannelEpoch(network={self.network!r}, "
            f"station={self.station!r}, location={self.locationcode!r} "
            f"channel={self.code!r}, epoch={self.epoch!r})>"
        )


class Routing(EpochMixin, LastSeenMixin, ORMBase):

    epoch_ref = Column(Integer, ForeignKey("epoch.id"), index=True)
    endpoint_ref = Column(Integer, ForeignKey("endpoint.id"), index=True)

    # TODO(damb): Make use of Association Proxy for cascades. See:
    # https://docs.sqlalchemy.org/en/14/orm/extensions/associationproxy.html
    # TODO(damb): Configure cascades
    epoch = relationship("Epoch", back_populates="endpoints")
    endpoint = relationship("Endpoint", back_populates="epochs")

    def __repr__(self):
        return (
            f"<Routing(url={self.endpoint.url!r}, "
            f"starttime={self.starttime!r}, endtime={self.endtime!r})>"
        )


class Endpoint(ORMBase):

    service_ref = Column(Integer, ForeignKey("service.id"), index=True)
    url = Column(String(LENGTH_URL), nullable=False)

    # many to many Epoch<->Endpoint
    epochs = relationship(
        "Routing", back_populates="endpoint", cascade="all, delete-orphan"
    )

    service = relationship("Service", back_populates="endpoints")

    def __repr__(self):
        return f"<Endpoint(url={self.url!r})>"


class Service(ORMBase):

    name = Column(String(LENGTH_STD_CODE), nullable=False, unique=True)

    endpoints = relationship(
        "Endpoint", back_populates="service", cascade="all, delete-orphan"
    )
    datacenters = relationship(
        "ServiceDataCenter",
        back_populates="service",
    )

    def __repr__(self):
        return f"<Service(name={self.name!r})>"


class DataCenter(DescriptionMixin, LastSeenMixin, ORMBase):

    name = Column(String(LENGTH_STD_CODE), unique=True)
    url = Column(String(LENGTH_URL), nullable=False)

    services = relationship("ServiceDataCenter", back_populates="datacenter")

    def __repr__(self):
        return f"<DataCenter(url={self.url!r}, name={self.name!r})>"


class ServiceDataCenter(ORMBase):
    service_ref = Column(Integer, ForeignKey("service.id"), index=True)
    datacenter_ref = Column(Integer, ForeignKey("datacenter.id"), index=True)
    enabled = Column(Boolean, default=True)

    # TODO(damb): Make use of Association Proxy for cascades. See:
    # https://docs.sqlalchemy.org/en/14/orm/extensions/associationproxy.html
    # TODO(damb): Configure cascades
    service = relationship("Service", back_populates="datacenters")
    datacenter = relationship("DataCenter", back_populates="services")


class VirtualChannelEpochGroup(CodeMixin, ORMBase):
    """
    ORM entity representing a *Virtual Network* in the context of
    :code:`eidaws-routing`.
    """

    virtual_channel_epochs = relationship(
        "VirtualChannelEpoch",
        back_populates="virtual_channel_epoch_group",
        cascade="all, delete-orphan",
    )

    def __repr__(self):
        return f"<VirtualChannelEpochGroup(code={self.code!r})>"


class VirtualChannelEpoch(EpochMixin, LastSeenMixin, ORMBase):
    """
    ORM entity representing a *Virtual Channel Epoch* in the context of
    :code:`eidaws-routing` virtual networks.
    """

    network_ref = Column(Integer, ForeignKey("network.id"), index=True)
    station_ref = Column(Integer, ForeignKey("station.id"), index=True)
    virtual_channel_epoch_group_ref = Column(
        Integer, ForeignKey("virtualchannelepochgroup.id"), index=True
    )
    channel = Column(String(LENGTH_CHANNEL_CODE), nullable=False, index=True)
    location = Column(String(LENGTH_LOCATION_CODE), nullable=False, index=True)

    station = relationship("Station", back_populates="virtual_channel_epochs")
    network = relationship("Network", back_populates="virtual_channel_epochs")
    virtual_channel_epoch_group = relationship(
        "VirtualChannelEpochGroup", back_populates="virtual_channel_epochs"
    )

    def __repr__(self):
        return (
            f"<VirtualChannelEpoch(network={self.network!r}, "
            f"station={self.station!r}, location={self.location!r}, "
            f"channel={self.channel!r}, "
            f"starttim={self.starttime!r}, endtime={self.endtime!r})>"
        )
