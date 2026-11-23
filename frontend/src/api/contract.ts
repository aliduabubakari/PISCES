import validate from "./generated/report-view.validator.js";
import type { ReportView } from "./generated/report-view";
import validateCampaign from "./generated/campaign-view.validator.js";
import type { CampaignView } from "./generated/campaign-view";

export function admitView(value: unknown): ReportView {
  if (!validate(value))
    throw new Error("Unsupported or malformed report-view contract.");
  return value;
}

export function admitWorkspaceView(value: unknown): ReportView | CampaignView {
  if (
    value &&
    typeof value === "object" &&
    "view_type" in value &&
    value.view_type === "campaign"
  ) {
    if (!validateCampaign(value))
      throw new Error("Unsupported or malformed campaign-view contract.");
    return value;
  }
  return admitView(value);
}
