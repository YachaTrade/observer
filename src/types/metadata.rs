#[derive(Debug, Clone, Default)]
pub struct TokenMetadata {
    pub image_uri: String,
    pub description: Option<String>,
    pub website: Option<String>,
    pub twitter: Option<String>,
    pub telegram: Option<String>,
    pub is_nsfw: bool,
}
