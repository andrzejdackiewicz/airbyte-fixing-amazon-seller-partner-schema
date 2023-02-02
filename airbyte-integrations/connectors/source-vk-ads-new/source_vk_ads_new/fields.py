AVAILABLE_FIELDS = {
    "banners": {
        "docs_url": "https://ads.vk.com/doc/api/object/Banner",
        "required": ["id"],
        "additional": [
            "created",
            "updated",
            "name",
            "status",
            "ad_group_id",
            "content",
            "delivery",
            "issues",
            "moderation_reasons",
            "moderation_status",
            "textblocks",
            "urls",
        ],
        "default": ["id", "ad_group_id", "campaign_id", "moderation_status"],
    },
    "ad_groups": {
        "docs_url": "https://ads.vk.com/doc/api/object/AdGroup",
        "required": ["id"],
        "additional": [
            "created",
            "updated",
            "name",
            "status",
            "package_id",
            "age_restrictions",
            "audit_pixels",
            "autobidding_mode",
            "banner_uniq_shows_limit",
            "banners",
            "budget_limit",
            "budget_limit_day",
            "date_end",
            "date_start",
            "delivery",
            "dynamic_banners_use_storelink",
            "dynamic_without_remarketing",
            "enable_offline_goals",
            "enable_utm",
            "issues",
            "language",
            "marketplace_app_client_id",
            "max_price",
            "objective",
            "package_priced_event_type",
            "price",
            "priced_goal",
            "pricelist_id",
            "sk_ad_campaign_id",
            "targetings",
            "uniq_shows_limit",
            "uniq_shows_period",
            "utm",
        ],
        "default": [
            "id",
            "name",
            "package_id",
        ],
    },
    "ad_plans": {
        "docs_url": "https://ads.vk.com/doc/api/object/AdPlan",
        "required": ["id"],
        "additional": [
            "created",
            "updated",
            "name",
            "status",
            "vkads_status",
            "ad_groups",
            "autobidding_mode",
            "budget_limit",
            "budget_limit_day",
            "date_start",
            "date_end",
            "max_price",
            "objective",
            "priced_goal",
            "pricelist_id",
        ],
        "default": [
            "id",
            "name",
        ],
    },
}
