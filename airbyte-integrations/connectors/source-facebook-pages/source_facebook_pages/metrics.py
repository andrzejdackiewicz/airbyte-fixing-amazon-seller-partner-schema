#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

PAGE_METRICS = [
    "page_tab_views_login_top_unique",
    "page_tab_views_login_top",
    "page_tab_views_logout_top",
    "page_total_actions",
    "page_cta_clicks_logged_in_total",
    "page_cta_clicks_logged_in_unique",
    "page_cta_clicks_by_site_logged_in_unique",
    "page_cta_clicks_by_age_gender_logged_in_unique",
    "page_cta_clicks_logged_in_by_country_unique",
    "page_cta_clicks_logged_in_by_city_unique",
    "page_call_phone_clicks_logged_in_unique",
    "page_call_phone_clicks_by_age_gender_logged_in_unique",
    "page_call_phone_clicks_logged_in_by_country_unique",
    "page_call_phone_clicks_logged_in_by_city_unique",
    "page_call_phone_clicks_by_site_logged_in_unique",
    "page_get_directions_clicks_logged_in_unique",
    "page_get_directions_clicks_by_age_gender_logged_in_unique",
    "page_get_directions_clicks_logged_in_by_country_unique",
    "page_get_directions_clicks_logged_in_by_city_unique",
    "page_get_directions_clicks_by_site_logged_in_unique",
    "page_website_clicks_logged_in_unique",
    "page_website_clicks_by_age_gender_logged_in_unique",
    "page_website_clicks_logged_in_by_country_unique",
    "page_website_clicks_logged_in_by_city_unique",
    "page_website_clicks_by_site_logged_in_unique",
    "page_engaged_users",
    "page_post_engagements",
    "page_consumptions",
    "page_consumptions_unique",
    "page_consumptions_by_consumption_type",
    "page_consumptions_by_consumption_type_unique",
    "page_places_checkin_total",
    "page_places_checkin_total_unique",
    "page_places_checkin_mobile",
    "page_places_checkin_mobile_unique",
    "page_places_checkins_by_age_gender",
    "page_places_checkins_by_locale",
    "page_places_checkins_by_country",
    "page_negative_feedback",
    "page_negative_feedback_unique",
    "page_negative_feedback_by_type",
    "page_negative_feedback_by_type_unique",
    "page_positive_feedback_by_type",
    "page_positive_feedback_by_type_unique",
    "page_fans_online",
    "page_fans_online_per_day",
    "page_fan_adds_by_paid_non_paid_unique",
    "page_impressions",
    "page_impressions_unique",
    "page_impressions_paid",
    "page_impressions_paid_unique",
    "page_impressions_organic",
    "page_impressions_organic_unique",
    "page_impressions_viral",
    "page_impressions_viral_unique",
    "page_impressions_nonviral",
    "page_impressions_nonviral_unique",
    "page_impressions_by_story_type",
    "page_impressions_by_story_type_unique",
    "page_impressions_by_city_unique",
    "page_impressions_by_country_unique",
    "page_impressions_by_locale_unique",
    "page_impressions_by_age_gender_unique",
    "page_impressions_frequency_distribution",
    "page_impressions_viral_frequency_distribution",
]

POST_METRICS = [
    "post_impressions",
    "post_impressions_unique",
    "post_impressions_paid",
    "post_impressions_paid_unique",
    "post_impressions_fan",
    "post_impressions_fan_unique",
    "post_impressions_fan_paid",
    "post_impressions_fan_paid_unique",
    "post_impressions_organic",
    "post_impressions_organic_unique",
    "post_impressions_viral",
    "post_impressions_viral_unique",
    "post_impressions_nonviral",
    "post_impressions_nonviral_unique",
    "post_impressions_by_story_type",
    "post_impressions_by_story_type_unique",
    "post_engaged_users",
    "post_negative_feedback",
    "post_negative_feedback_unique",
    "post_negative_feedback_by_type",
    "post_negative_feedback_by_type_unique",
    "post_engaged_fan",
    "post_clicks",
    "post_clicks_unique",
    "post_clicks_by_type",
    "post_clicks_by_type_unique",
    "post_reactions_by_type_total",
]

PAGE_FIELDS = ",".join(
    [
        "id",
        "about",
        "ad_campaign",
        "affiliation",
        "app_id",
        "artists_we_like",
        "attire",
        "awards",
        "band_interests",
        "band_members",
        "bio",
        "birthday",
        "booking_agent",
        "built",
        "can_checkin",
        "can_post",
        "category",
        "category_list",
        "checkins",
        "company_overview",
        "connected_page_backed_instagram_account",
        "contact_address",
        "country_page_likes",
        "cover",
        "culinary_team",
        "current_location",
        "delivery_and_pickup_option_info",
        "description",
        "description_html",
        "differently_open_offerings",
        "directed_by",
        "display_subtext",
        "displayed_message_response_time",
        "emails",
        "engagement",
        "fan_count",
        "featured_video",
        "features",
        "followers_count",
        "food_styles",
        "founded",
        "general_info",
        "general_manager",
        "genre",
        "global_brand_page_name",
        "global_brand_root_id",
        "has_added_app",
        "has_transitioned_to_new_page_experience",
        "has_whatsapp_business_number",
        "has_whatsapp_number",
        "hometown",
        "hours",
        "impressum",
        "influences",
        "is_always_open",
        "is_chain",
        "is_community_page",
        "is_eligible_for_branded_content",
        "is_messenger_bot_get_started_enabled",
        "is_messenger_platform_bot",
        "is_owned",
        "is_permanently_closed",
        "is_published",
        "is_unclaimed",
        "is_webhooks_subscribed",
        "leadgen_tos_acceptance_time",
        "leadgen_tos_accepted",
        "leadgen_tos_accepting_user",
        "link",
        "location",
        "members",
        "merchant_review_status",
        "messenger_ads_default_icebreakers",
        "messenger_ads_default_page_welcome_message",
        "messenger_ads_default_quick_replies",
        "messenger_ads_quick_replies_type",
        "mission",
        "mpg",
        "name",
        "name_with_location_descriptor",
        "network",
        "new_like_count",
        "offer_eligible",
        "overall_star_rating",
        "page_token",
        "parking",
        "payment_options",
        "personal_info",
        "personal_interests",
        "pharma_safety_info",
        "phone",
        "pickup_options",
        "place_type",
        "plot_outline",
        "preferred_audience",
        "press_contact",
        "price_range",
        "privacy_info_url",
        "produced_by",
        "products",
        "promotion_eligible",
        "promotion_ineligible_reason",
        "public_transit",
        "rating_count",
        "record_label",
        "release_date",
        "restaurant_services",
        "restaurant_specialties",
        "schedule",
        "screenplay_by",
        "season",
        "single_line_address",
        "starring",
        "start_info",
        "store_code",
        "store_location_descriptor",
        "store_number",
        "studio",
        "supports_donate_button_in_live_video",
        "talking_about_count",
        "temporary_status",
        "unread_message_count",
        "unread_notif_count",
        "unseen_message_count",
        "username",
        "verification_status",
        "voip_info",
        "website",
        "were_here_count",
        "whatsapp_number",
        "written_by",
        "agencies",
        "albums",
        "blocked",
        "call_to_actions",
        "canvas_elements",
        "commerce_merchant_settings",
        "events",
        "feed",
        "global_brand_children",
        "groups",
        "image_copyrights",
        "indexed_videos",
        # "insights_exports",    Tried accessing nonexisting field (insights_exports) on node type (Page)
        "instagram_accounts",
        "likes",
        "live_videos",
        "locations",
        "nativeoffers",
        "page_backed_instagram_accounts",
        "photos",
        "posts",
        "product_catalogs",
        "published_posts",
        "ratings",
        "roles",
        "rtb_dynamic_posts",
        "scheduled_posts",
        "settings",
        "tabs",
        "tagged",
        "video_lists",
        "videos",
        "visitor_posts",
    ]
)

POST_FIELDS = ",".join(
    [
        "id",
        "actions",
        "admin_creator",
        "application",
        "backdated_time",
        "call_to_action",
        "can_reply_privately",
        "child_attachments",
        "comments_mirroring_domain",
        "coordinates",
        "created_time",
        "event",
        "expanded_height",
        "expanded_width",
        "feed_targeting",
        "from",
        "height",
        "icon",
        "instagram_eligibility",
        "is_eligible_for_promotion",
        "is_expired",
        "is_hidden",
        "is_inline_created",
        "is_instagram_eligible",
        "is_popular",
        "is_published",
        "is_spherical",
        "message",
        "message_tags",
        "multi_share_end_card",
        "multi_share_optimized",
        "parent_id",
        "permalink_url",
        "place",
        "privacy",
        "promotable_id",
        "promotion_status",
        "properties",
        "scheduled_publish_time",
        "shares",
        "status_type",
        "story",
        "story_tags",
        "subscribed",
        "target",
        "targeting",
        "timeline_visibility",
        "updated_time",
        "via",
        "video_buying_eligibility",
        "width",
        "comments",
        "dynamic_posts",
        "likes",
        "reactions",
        "sharedposts",
        "sponsor_tags",
        "to",
    ]
)
