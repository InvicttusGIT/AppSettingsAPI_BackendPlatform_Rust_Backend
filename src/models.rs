use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppSettingSimpleItem {
    pub platform: String,
    pub device: String,
    pub country: String,
    pub app_settings: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppSettingsResponse {
    pub app_name: String,
    pub version_number: String,
    pub status: String,
    pub settings: Vec<AppSettingSimpleItem>,
}

