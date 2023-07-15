from dataclasses import dataclass
import app.models.codes


@dataclass
class AutomapDecision:
    target_code: app.models.codes.Code
    reason: str
