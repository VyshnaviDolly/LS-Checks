from dataclasses import dataclass
from enum import Enum


class CheckType(Enum):
    CURRENT_DAY = "current_day"
    UPCOMING_DAY = "upcoming_day"
    CURRENT_DAY_DOUBLE = "current_day_double_check"
    UPCOMING_DAY_DOUBLE = "upcoming_day_double_check"


class Sport(Enum):
    BASKETBALL = "Basketball"
    VOLLEYBALL = "Volleyball"
    HANDBALL = "Handball"
    ICE_HOCKEY = "Ice Hockey"
    BASEBALL = "Baseball"


# Which sports each check type processes
CHECK_SPORT_SCOPE: dict[CheckType, set[Sport]] = {
    CheckType.CURRENT_DAY:        {Sport.BASKETBALL, Sport.VOLLEYBALL, Sport.HANDBALL, Sport.ICE_HOCKEY, Sport.BASEBALL},
    CheckType.UPCOMING_DAY:       {Sport.BASKETBALL, Sport.VOLLEYBALL, Sport.HANDBALL, Sport.ICE_HOCKEY},
    CheckType.CURRENT_DAY_DOUBLE: {Sport.BASKETBALL, Sport.VOLLEYBALL, Sport.HANDBALL, Sport.ICE_HOCKEY},
    CheckType.UPCOMING_DAY_DOUBLE:{Sport.BASKETBALL, Sport.VOLLEYBALL, Sport.HANDBALL, Sport.ICE_HOCKEY},
}

# Whether a check type uses LBC/Main reference comparison
CHECK_USES_REFERENCE: dict[CheckType, bool] = {
    CheckType.CURRENT_DAY:         True,
    CheckType.UPCOMING_DAY:        False,
    CheckType.CURRENT_DAY_DOUBLE:  True,
    CheckType.UPCOMING_DAY_DOUBLE: True,
}


@dataclass
class PipelineConfig:
    check_type: CheckType = CheckType.CURRENT_DAY
    main_reference_path: str | None = None
    blacklist_path: str | None = None
    output_dir: str = "output"
    upload_dir: str = "uploads"
    fuzzy_threshold: float = 0.85
    time_tolerance_minutes: int = 5
    date_override: str | None = None   # YYYY-MM-DD start date
    end_date_override: str | None = None  # YYYY-MM-DD end date
