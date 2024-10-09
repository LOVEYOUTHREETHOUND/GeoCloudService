from pydantic import BaseModel, Field
from enum import Enum
from typing import Optional


class ShapeTypeEnum(str, Enum):
    mountain = "mountain"
    river = "river"
    forest = "forest"
    soil = "soil"
    grass = "grass"


class ShapeQueryModel(BaseModel):
    typ: ShapeTypeEnum


class DeviceIDModel(BaseModel):
    device_id: str
    did: str


class OnlineModel(BaseModel):
    dids: list


class WarningModel(BaseModel):
    wid: int = Field(default=None, title="Warning id (1 - 8)")


class HandleWarningModel(BaseModel):
    warning_hash: str
    handle: int


class ThresholdModel(BaseModel):
    sensor: str = Field(default=None, title="Sensor type")


class ThresholdPostModel(BaseModel):
    high_level1: float = Field(default=None)
    high_level2: float = Field(default=None)
    high_level3: float = Field(default=None)
    high_level4: float = Field(default=None)
    high_message: str = Field(default=None)
    low_level1: float = Field(default=None)
    low_level2: float = Field(default=None)
    low_level3: float = Field(default=None)
    low_level4: float = Field(default=None)
    low_message: str = Field(default=None)


class TimespanQueryModel(BaseModel):
    startts: int = Field(default=None, title="Start timestamp")
    endts: int = Field(default=None, title="End timestamp")


class SensingDataQueryModel(BaseModel):
    startts: int = Field(default=None, title="Start timestamp")
    endts: int = Field(default=None, title="End timestamp")
    count: int = Field(default=None, title="Number of data to fetch")
    sensors: list = Field(title="Sensor list")


class SensingDataSensorModel(BaseModel):
    sensors: list = Field(title="Sensor list")


class SensingDataModel(BaseModel):
    dids: list = Field(title="Device list")
    sensors: list = Field(title="Sensor list")
    startts: int = Field(default=None, title="Start timestamp")
    endts: int = Field(default=None, title="End timestamp")


class AvgSensingDataModel(BaseModel):
    dids: list = Field(title="Device list")
    sensors: list = Field(title="Sensor list")
    startts: int = Field(default=None, title="Start timestamp")
    endts: int = Field(default=None, title="End timestamp")
    interval: str = Field(default="5m", title="Time interval")


class BatchWarningHandle(BaseModel):
    did: str = Field(default=None, title="Which did")
    sensor: str = Field(default=None, title="Which sensor")
    handle: int


class WarningCount(BaseModel):
    today: str = Field(default=None, title="Time string for current day, format: YYYY-MM-DD")


class ScreenDataModel(BaseModel):
    #screen_id: str = Field(title="ID for screen device")
    text: str = Field(default=None, title="Shown text for screen")
    sensing_did: str = Field(title="Did for sensing device")


class ReserveData(BaseModel):
    reserve_name: str = Field(title="Name for reserve")
    latitude: float = Field(title="Latitude for reserve")
    longitude: float = Field(title="Longitude for reserve")

