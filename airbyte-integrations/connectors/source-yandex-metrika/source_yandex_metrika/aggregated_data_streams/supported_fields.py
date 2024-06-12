from ..fields import prepare_fields_list

_RAW_GROUP_FIELDS = [
    "ym:s:<attribution>TrafficSource",
    "ym:s:<attribution>SourceEngine",
    "ym:s:<attribution>AdvEngine",
    "ym:s:<attribution>ReferalSource",
    "ym:s:<attribution>Messenger",
    "ym:s:<attribution>RecommendationSystem",
    "ym:s:<attribution>DirectClickOrder",
    "ym:s:<attribution>DirectBannerGroup",
    "ym:s:<attribution>DirectClickBanner",
    "ym:s:<attribution>DirectPhraseOrCond",
    "ym:s:<attribution>DirectPlatformType",
    "ym:s:<attribution>DirectPlatform",
    "ym:s:<attribution>DirectSearchPhrase",
    "ym:s:<attribution>DirectConditionType",
    "ym:s:<attribution>Currency",
    "ym:s:experimentAB<experiment_ab>",
    "ym:s:<attribution>DisplayCampaign",
    "ym:s:marketSearchPhrase",
    "ym:s:<attribution>SearchEngineRoot",
    "ym:s:<attribution>SearchEngine",
    "ym:s:<attribution>SearchPhrase",
    "ym:s:<attribution>SocialNetwork",
    "ym:s:<attribution>SocialNetworkProfile",
    "ym:s:referer",
    "ym:s:refererProto",
    "ym:s:refererDomain",
    "ym:s:refererPathFull",
    "ym:s:refererPath",
    "ym:s:refererPathLevel1",
    "ym:s:refererPathLevel2",
    "ym:s:refererPathLevel3",
    "ym:s:refererPathLevel4",
    "ym:s:refererPathLevel5",
    "ym:s:externalReferer",
    "ym:s:externalRefererProto",
    "ym:s:externalRefererDomain",
    "ym:s:externalRefererPathFull",
    "ym:s:externalRefererPath",
    "ym:s:externalRefererPathLevel1",
    "ym:s:externalRefererPathLevel2",
    "ym:s:externalRefererPathLevel3",
    "ym:s:externalRefererPathLevel4",
    "ym:s:externalRefererPathLevel5",
    "ym:s:from",
    "ym:s:UTMCampaign",
    "ym:s:UTMContent",
    "ym:s:UTMMedium",
    "ym:s:UTMSource",
    "ym:s:UTMTerm",
    "ym:s:openstatAd",
    "ym:s:openstatCampaign",
    "ym:s:openstatService",
    "ym:s:openstatSource",
    "ym:s:hasGCLID",
    "ym:s:goal",
    "ym:s:goal<goal_id>IsReached",
    "ym:s:pageViewsInterval",
    "ym:s:pageViews",
    "ym:s:visitDurationInterval",
    "ym:s:visitDuration",
    "ym:s:bounce",
    "ym:s:isRobot",
    "ym:s:startURL",
    "ym:s:startURLProto",
    "ym:s:topLevelDomain",
    "ym:s:startURLDomain",
    "ym:s:startURLPathFull",
    "ym:s:startURLPath",
    "ym:s:startURLPathLevel1",
    "ym:s:startURLPathLevel2",
    "ym:s:startURLPathLevel3",
    "ym:s:startURLPathLevel4",
    "ym:s:startURLPathLevel5",
    "ym:s:endURL",
    "ym:s:endURLProto",
    "ym:s:endURLDomain",
    "ym:s:endURLPathFull",
    "ym:s:endURLPath",
    "ym:s:endURLPathLevel1",
    "ym:s:endURLPathLevel2",
    "ym:s:endURLPathLevel3",
    "ym:s:endURLPathLevel4",
    "ym:s:endURLPathLevel5",
    "ym:s:paramsLevel1",
    "ym:s:paramsLevel2",
    "ym:s:paramsLevel3",
    "ym:s:paramsLevel4",
    "ym:s:paramsLevel5",
    "ym:s:paramsLevel6",
    "ym:s:paramsLevel7",
    "ym:s:paramsLevel8",
    "ym:s:paramsLevel9",
    "ym:s:paramsLevel10",
    "ym:s:date",
    "ym:s:datePeriod<group>",
    "ym:s:startOfYear",
    "ym:s:startOfQuarter",
    "ym:s:startOfMonth",
    "ym:s:startOfWeek",
    "ym:s:startOfHour",
    "ym:s:startOfDekaminute",
    "ym:s:startOfMinute",
    "ym:s:dateTime",
    "ym:s:year",
    "ym:s:month",
    "ym:s:dayOfMonth",
    "ym:s:dayOfWeek",
    "ym:s:hour",
    "ym:s:minute",
    "ym:s:dekaminute",
    "ym:s:hourMinute",
    "ym:s:isNewUser",
    "ym:s:userVisitsInterval",
    "ym:s:userVisits",
    "ym:s:daysSinceFirstVisitInterval",
    "ym:s:daysSinceFirstVisit",
    "ym:s:daysSincePreviousVisitInterval",
    "ym:s:daysSincePreviousVisit",
    "ym:s:userVisitsPeriodInterval",
    "ym:s:userVisitsPeriod",
    "ym:s:firstVisitDate",
    "ym:s:firstVisitDatePeriod<group>",
    "ym:s:firstVisitStartOfYear",
    "ym:s:firstVisitStartOfQuarter",
    "ym:s:firstVisitStartOfMonth",
    "ym:s:firstVisitStartOfWeek",
    "ym:s:firstVisitStartOfHour",
    "ym:s:firstVisitStartOfDekaminute",
    "ym:s:firstVisitStartOfMinute",
    "ym:s:firstVisitDateTime",
    "ym:s:firstVisitYear",
    "ym:s:firstVisitMonth",
    "ym:s:firstVisitDayOfMonth",
    "ym:s:firstVisitDayOfWeek",
    "ym:s:firstVisitHour",
    "ym:s:firstVisitMinute",
    "ym:s:firstVisitDekaminute",
    "ym:s:firstVisitHourMinute",
    "ym:s:previousVisitDate",
    "ym:s:previousVisitDatePeriod<group>",
    "ym:s:previousVisitStartOfYear",
    "ym:s:previousVisitStartOfQuarter",
    "ym:s:previousVisitStartOfMonth",
    "ym:s:previousVisitStartOfWeek",
    "ym:s:previousVisitYear",
    "ym:s:previousVisitMonth",
    "ym:s:previousVisitDayOfMonth",
    "ym:s:previousVisitDayOfWeek",
    "ym:s:gender",
    "ym:s:ageInterval",
    "ym:s:interest",
    "ym:s:interest2d1",
    "ym:s:interest2d2",
    "ym:s:interest2d3",
    "ym:s:clientID",
    "ym:s:regionContinent",
    "ym:s:regionCountry",
    "ym:s:regionDistrict",
    "ym:s:regionArea",
    "ym:s:regionCity",
    "ym:s:regionCitySize",
    "ym:s:browserLanguage",
    "ym:s:browserCountry",
    "ym:s:clientTimeZone",
    "ym:s:ipAddress",
    "ym:s:deviceCategory",
    "ym:s:mobilePhone",
    "ym:s:mobilePhoneModel",
    "ym:s:operatingSystemRoot",
    "ym:s:operatingSystem",
    "ym:s:browser",
    "ym:s:browserAndVersionMajor",
    "ym:s:browserAndVersion",
    "ym:s:cookieEnabled",
    "ym:s:javascriptEnabled",
    "ym:s:browserEngine",
    "ym:s:browserEngineVersion1",
    "ym:s:browserEngineVersion2",
    "ym:s:browserEngineVersion3",
    "ym:s:browserEngineVersion4",
    "ym:s:screenFormat",
    "ym:s:screenColors",
    "ym:s:screenOrientation",
    "ym:s:screenResolution",
    "ym:s:screenWidth",
    "ym:s:screenHeight",
    "ym:s:physicalScreenResolution",
    "ym:s:physicalScreenWidth",
    "ym:s:physicalScreenHeight",
    "ym:s:windowClientArea",
    "ym:s:windowClientWidth",
    "ym:s:windowClientHeight",
    "ym:s:productID",
    "ym:s:productName",
    "ym:s:productBrand",
    "ym:s:productVariant",
    "ym:s:productPosition",
    "ym:s:productPrice",
    "ym:s:productSum",
    "ym:s:productCurrency",
    "ym:s:purchaseID",
    "ym:s:purchaseCoupon",
    "ym:s:purchaseRevenue",
    "ym:s:productCategoryLevel1",
    "ym:s:productCategoryLevel2",
    "ym:s:productCategoryLevel3",
    "ym:s:productCategoryLevel4",
    "ym:s:productCategoryLevel5",
    "ym:s:productCoupon",
    "ym:s:productIDCart",
    "ym:s:productNameCart",
    "ym:s:productBrandCart",
    "ym:s:PProductID",
    "ym:s:PProductName",
    "ym:s:PProductBrand",
    "ym:s:purchaseExistsVisit",
    "ym:s:purchaseRevenueVisit",
    "ym:s:offlineCallTag",
    "ym:s:offlineCallMissed",
    "ym:s:offlineCallFirstTimeCaller",
    "ym:s:offlineCallHoldDuration",
    "ym:s:offlineCallTalkDuration",
    "ym:s:yanPageID",
    "ym:s:yanBlockID",
    "ym:s:yanBlockSize",
    "ym:s:yanUrl",
    "ym:s:yanUrlPathLevel1",
    "ym:s:yanUrlPathLevel2",
    "ym:s:yanUrlPathLevel3",
    "ym:s:yanUrlPathLevel4",
    "ym:s:adfoxSite",
    "ym:s:adfoxSection",
    "ym:s:adfoxPlace",
    "ym:s:adfoxBanner",
    "ym:s:adfoxCampaign",
    "ym:s:adfoxOwner",
    "ym:s:adfoxYanPage",
    "ym:s:adfoxYanImp",
    "ym:s:adfoxHeaderBidding",
    "ym:s:adfoxLevel1",
    "ym:s:adfoxLevel2",
    "ym:s:adfoxUrlPathLevel1",
    "ym:s:adfoxUrlPathLevel2",
    "ym:s:adfoxUrlPathLevel3",
    "ym:s:adfoxUrlPathLevel4",
    "ym:s:publisherArticleTitle",
    "ym:s:publisherArticleRubric",
    "ym:s:publisherArticleRubric2",
    "ym:s:publisherArticlePublishedDate",
    "ym:s:publisherArticleURL",
    "ym:s:publisherArticleScrollDown",
    "ym:s:publisherArticleHasFullScroll",
    "ym:s:publisherArticleReadPart",
    "ym:s:publisherArticleHasFullRead",
    "ym:s:publisherArticleFromTitle",
    "ym:s:publisherArticleHasRecircled",
    "ym:s:publisherArticleAuthor",
    "ym:s:publisherArticleTopic",
    "ym:s:publisherLongArticle",
    "ym:s:publisherTrafficSource",
    "ym:s:publisherTrafficSource2",
    "ym:s:publisherPageFormat",
    "ym:s:vacuumSurface",
    "ym:s:vacuumEvent",
    "ym:s:vacuumOrganization",
    "ym:pv:title",
    "ym:pv:URL",
    "ym:pv:URLProto",
    "ym:pv:URLDomain",
    "ym:pv:URLPathFull",
    "ym:pv:URLPath",
    "ym:pv:URLPathLevel1",
    "ym:pv:URLPathLevel2",
    "ym:pv:URLPathLevel3",
    "ym:pv:URLPathLevel4",
    "ym:pv:URLPathLevel5",
    "ym:pv:URLParamName",
    "ym:pv:URLParamNameAndValue",
    "ym:pv:referer",
    "ym:pv:refererProto",
    "ym:pv:refererDomain",
    "ym:pv:refererPathFull",
    "ym:pv:refererPath",
    "ym:pv:refererPathLevel1",
    "ym:pv:refererPathLevel2",
    "ym:pv:refererPathLevel3",
    "ym:pv:refererPathLevel4",
    "ym:pv:refererPathLevel5",
    "ym:pv:from",
    "ym:pv:UTMCampaign",
    "ym:pv:UTMContent",
    "ym:pv:UTMMedium",
    "ym:pv:UTMSource",
    "ym:pv:UTMTerm",
    "ym:pv:openstatAd",
    "ym:pv:openstatCampaign",
    "ym:pv:openstatService",
    "ym:pv:openstatSource",
    "ym:pv:hasGCLID",
    "ym:pv:date",
    "ym:pv:datePeriod<group>",
    "ym:pv:startOfYear",
    "ym:pv:startOfQuarter",
    "ym:pv:startOfMonth",
    "ym:pv:startOfWeek",
    "ym:pv:startOfHour",
    "ym:pv:startOfDekaminute",
    "ym:pv:startOfMinute",
    "ym:pv:dateTime",
    "ym:pv:year",
    "ym:pv:month",
    "ym:pv:dayOfMonth",
    "ym:pv:dayOfWeek",
    "ym:pv:hour",
    "ym:pv:minute",
    "ym:pv:dekaminute",
    "ym:pv:hourMinute",
    "ym:pv:gender",
    "ym:pv:ageInterval",
    "ym:pv:regionContinent",
    "ym:pv:regionCountry",
    "ym:pv:regionDistrict",
    "ym:pv:regionArea",
    "ym:pv:regionCity",
    "ym:pv:regionCitySize",
    "ym:pv:browserLanguage",
    "ym:pv:browserCountry",
    "ym:pv:clientTimeZone",
    "ym:pv:ipAddress",
    "ym:pv:deviceCategory",
    "ym:pv:mobilePhone",
    "ym:pv:mobilePhoneModel",
    "ym:pv:operatingSystemRoot",
    "ym:pv:operatingSystem",
    "ym:pv:browser",
    "ym:pv:browserAndVersionMajor",
    "ym:pv:browserAndVersion",
    "ym:pv:cookieEnabled",
    "ym:pv:javascriptEnabled",
    "ym:pv:browserEngine",
    "ym:pv:browserEngineVersion1",
    "ym:pv:browserEngineVersion2",
    "ym:pv:browserEngineVersion3",
    "ym:pv:browserEngineVersion4",
    "ym:pv:screenFormat",
    "ym:pv:screenColors",
    "ym:pv:screenOrientation",
    "ym:pv:screenResolution",
    "ym:pv:screenWidth",
    "ym:pv:screenHeight",
    "ym:pv:physicalScreenResolution",
    "ym:pv:physicalScreenWidth",
    "ym:pv:physicalScreenHeight",
    "ym:pv:windowClientArea",
    "ym:pv:windowClientWidth",
    "ym:pv:windowClientHeight",
    "ym:ad:<attribution>DirectOrder",
    "ym:ad:<attribution>DirectBannerGroup",
    "ym:ad:<attribution>DirectBanner",
    "ym:ad:<attribution>DirectPhraseOrCond",
    "ym:ad:<attribution>DirectPlatformType",
    "ym:ad:<attribution>DirectPlatform",
    "ym:ad:<attribution>DirectSearchPhrase",
    "ym:ad:<attribution>DirectConditionType",
    "ym:ad:<attribution>Currency",
    "ym:ad:<attribution>DisplayCampaign",
    "ym:up:paramsLevel1",
    "ym:up:paramsLevel2",
    "ym:up:paramsLevel3",
    "ym:up:paramsLevel4",
    "ym:up:paramsLevel5",
    "ym:ev:<attribution>ExpenseSource",
    "ym:ev:<attribution>ExpenseMedium",
    "ym:ev:<attribution>ExpenseCampaign",
    "ym:ev:<attribution>ExpenseTerm",
    "ym:ev:<attribution>ExpenseContent",
]

_RAW_METRICS_FIELDS = [
    ("ym:s:visits", "integer"),
    ("ym:s:pageviews", "integer"),
    ("ym:s:users", "integer"),
    ("ym:s:bounceRate", "number"),
    ("ym:s:pageDepth", "number"),
    ("ym:s:avgVisitDurationSeconds", "integer"),
    ("ym:s:visitsPerDay", "number"),
    ("ym:s:visitsPerHour", "number"),
    ("ym:s:visitsPerMinute", "number"),
    ("ym:s:robotPercentage", "number"),
    ("ym:s:goal<goal_id>conversionRate", "number"),
    ("ym:s:goal<goal_id>userConversionRate", "number"),
    ("ym:s:goal<goal_id>users", "integer"),
    ("ym:s:goal<goal_id>visits", "integer"),
    ("ym:s:goal<goal_id>reaches", "integer"),
    ("ym:s:goal<goal_id>reachesPerUser", "number"),
    ("ym:s:goal<goal_id>revenue", "number"),
    ("ym:s:goal<goal_id><currency>revenue", "number"),
    ("ym:s:goal<goal_id>converted<currency>Revenue", "number"),
    ("ym:s:goal<goal_id>ecommercePurchases", "number"),
    ("ym:s:goal<goal_id>ecommerce<currency>ConvertedRevenue", "number"),
    ("ym:s:anyGoalConversionRate", "number"),
    ("ym:s:sumGoalReachesAny", "integer"),
    ("ym:s:paramsNumber", "integer"),
    ("ym:s:sumParams", "integer"),
    ("ym:s:avgParams", "number"),
    ("ym:s:percentNewVisitors", "number"),
    ("ym:s:newUsers", "integer"),
    ("ym:s:newUserVisitsPercentage", "number"),
    ("ym:s:upToDaySinceFirstVisitPercentage", "number"),
    ("ym:s:upToWeekSinceFirstVisitPercentage", "number"),
    ("ym:s:upToMonthSinceFirstVisitPercentage", "number"),
    ("ym:s:upToQuarterSinceFirstVisitPercentage", "number"),
    ("ym:s:upToYearSinceFirstVisitPercentage", "number"),
    ("ym:s:overYearSinceFirstVisitPercentage", "number"),
    ("ym:s:oneVisitPerUserPercentage", "number"),
    ("ym:s:upTo3VisitsPerUserPercentage", "number"),
    ("ym:s:upTo7VisitsPerUserPercentage", "number"),
    ("ym:s:upTo31VisitsPerUserPercentage", "number"),
    ("ym:s:over32VisitsPerUserPercentage", "number"),
    ("ym:s:oneDayBetweenVisitsPercentage", "number"),
    ("ym:s:upToWeekBetweenVisitsPercentage", "number"),
    ("ym:s:upToMonthBetweenVisitsPercentage", "number"),
    ("ym:s:overMonthBetweenVisitsPercentage", "number"),
    ("ym:s:upToDayUserRecencyPercentage", "number"),
    ("ym:s:upToWeekUserRecencyPercentage", "number"),
    ("ym:s:upToMonthUserRecencyPercentage", "number"),
    ("ym:s:upToQuarterUserRecencyPercentage", "number"),
    ("ym:s:upToYearUserRecencyPercentage", "number"),
    ("ym:s:overYearUserRecencyPercentage", "number"),
    ("ym:s:avgDaysBetweenVisits", "number"),
    ("ym:s:avgDaysSinceFirstVisit", "number"),
    ("ym:s:userRecencyDays", "number"),
    ("ym:s:manPercentage", "number"),
    ("ym:s:womanPercentage", "number"),
    ("ym:s:under18AgePercentage", "number"),
    ("ym:s:upTo24AgePercentage", "number"),
    ("ym:s:upTo34AgePercentage", "number"),
    ("ym:s:upTo44AgePercentage", "number"),
    ("ym:s:over44AgePercentage", "number"),
    ("ym:s:cookieEnabledPercentage", "number"),
    ("ym:s:jsEnabledPercentage", "number"),
    ("ym:s:mobilePercentage", "number"),
    ("ym:s:productImpressions", "integer"),
    ("ym:s:productImpressionsUniq", "integer"),
    ("ym:s:productBasketsQuantity", "integer"),
    ("ym:s:productBasketsPrice", "number"),
    ("ym:s:productBasketsUniq", "integer"),
    ("ym:s:productPurchasedQuantity", "integer"),
    ("ym:s:productPurchasedPrice", "number"),
    ("ym:s:productPurchasedUniq", "integer"),
    ("ym:s:ecommercePurchases", "integer"),
    ("ym:s:ecommerceRevenue", "number"),
    ("ym:s:ecommerceRevenuePerVisit", "number"),
    ("ym:s:ecommerceRevenuePerPurchase", "number"),
    ("ym:s:ecommerce<currency>ConvertedRevenue", "number"),
    ("ym:s:offlineCalls", "integer"),
    ("ym:s:offlineCallsMissed", "integer"),
    ("ym:s:offlineCallsMissedPercentage", "number"),
    ("ym:s:offlineCallsFirstTimeCaller", "integer"),
    ("ym:s:offlineCallsFirstTimeCallerPercentage", "number"),
    ("ym:s:offlineCallsUniq", "integer"),
    ("ym:s:offlineCallTalkDurationAvg", "number"),
    ("ym:s:offlineCallHoldDurationTillAnswerAvg", "number"),
    ("ym:s:offlineCallHoldDurationTillMissAvg", "number"),
    ("ym:s:offlineCallDurationAvg", "number"),
    ("ym:s:offlineCallRevenueAvg", "number"),
    ("ym:s:offlineCallRevenue", "number"),
    ("ym:s:yanRequests", "integer"),
    ("ym:s:yanRenders", "integer"),
    ("ym:s:yanShows", "integer"),
    ("ym:s:yanPartnerPrice", "number"),
    ("ym:s:yanCPMV", "number"),
    ("ym:s:yanECPM", "number"),
    ("ym:s:yanRPM", "number"),
    ("ym:s:yanVisibility", "number"),
    ("ym:s:yanARPU", "number"),
    ("ym:s:yanRendersPerUser", "number"),
    ("ym:s:yanRevenuePerVisit", "number"),
    ("ym:s:yanRevenuePerHit", "number"),
    ("ym:s:yanRendersPerVisit", "number"),
    ("ym:s:yanRendersPerHit", "number"),
    ("ym:s:adfoxRequests", "integer"),
    ("ym:s:adfoxRenders", "integer"),
    ("ym:s:adfoxRendersDefault", "integer"),
    ("ym:s:adfoxShows", "integer"),
    ("ym:s:adfoxClicks", "integer"),
    ("ym:s:adfoxVisibility", "number"),
    ("ym:s:adfoxRendersPerUser", "number"),
    ("ym:s:adfoxRendersPerVisit", "number"),
    ("ym:s:adfoxRendersPerHit", "number"),
    ("ym:s:adfoxPrice", "number"),
    ("ym:s:adfoxCPMV", "number"),
    ("ym:s:adfoxECPM", "number"),
    ("ym:s:adfoxRPM", "number"),
    ("ym:s:adfoxARPU", "number"),
    ("ym:s:adfoxPricePerUser", "number"),
    ("ym:s:adfoxPricePerVisit", "number"),
    ("ym:s:adfoxPricePerHit", "number"),
    ("ym:s:adfoxPriceStrict", "number"),
    ("ym:s:adfoxPriceYan", "number"),
    ("ym:s:adfoxPriceGoogle", "number"),
    ("ym:s:adfoxPriceHeaderBidding", "number"),
    ("ym:s:sumPublisherArticleInvolvedTimeSeconds", "number"),
    ("ym:s:avgPublisherArticleInvolvedTimeSeconds", "number"),
    ("ym:s:publisherviews", "integer"),
    ("ym:s:publisherviewsPerDay", "number"),
    ("ym:s:publisherviewsPerHour", "number"),
    ("ym:s:publisherviewsPerMinute", "number"),
    ("ym:s:publisherusers", "integer"),
    ("ym:s:publisherArticleUsersRecircled", "number"),
    ("ym:s:publisherArticleRecirculation", "number"),
    ("ym:s:publisherViewsFullScroll", "integer"),
    ("ym:s:publisherArticleViewsFullScrollShare", "number"),
    ("ym:s:publisherViewsFullRead", "integer"),
    ("ym:s:publisherArticleFullReadShare", "number"),
    ("ym:s:publisherMobileOrTabletViews", "integer"),
    ("ym:s:publisherMobileOrTabletViewsShare", "number"),
    ("ym:s:vacuumevents", "integer"),
    ("ym:s:vacuumusers", "integer"),
    ("ym:s:vacuumeventsPerUser", "number"),
    ("ym:s:affinityIndexInterests", "number"),
    ("ym:s:affinityIndexInterests2", "number"),
    ("ym:s:GCLIDPercentage", "number"),
    ("ym:pv:pageviews", "integer"),
    ("ym:pv:users", "integer"),
    ("ym:pv:pageviewsPerDay", "number"),
    ("ym:pv:pageviewsPerHour", "number"),
    ("ym:pv:pageviewsPerMinute", "number"),
    ("ym:pv:cookieEnabledPercentage", "number"),
    ("ym:pv:jsEnabledPercentage", "number"),
    ("ym:pv:mobilePercentage", "number"),
    ("ym:ad:visits", "integer"),
    ("ym:ad:clicks", "integer"),
    ("ym:ad:<currency>AdCost", "number"),
    ("ym:ad:<currency>AdCostPerVisit", "number"),
    ("ym:ad:goal<goal_id><currency>CPA", "number"),
    ("ym:ad:goal<goal_id><currency>AdCostPerVisit", "number"),
    ("ym:up:params", "integer"),
    ("ym:up:users", "integer"),
    ("ym:ev:expenses<currency>", "number"),
    ("ym:ev:expenseClicks", "integer"),
    ("ym:ev:expense<currency>CPC", "number"),
    ("ym:ev:expense<currency>EcommerceROI", "number"),
    ("ym:ev:goal<goal_id>expense<currency>ROI", "number"),
    ("ym:ev:expense<currency>EcommerceCPA", "number"),
    ("ym:ev:goal<goal_id>expense<currency>ReachCPA", "number"),
    ("ym:ev:goal<goal_id>expense<currency>VisitCPA", "number"),
    ("ym:ev:goal<goal_id>expense<currency>UserCPA", "number"),
    ("ym:ev:expense<currency>EcommerceCRR", "number"),
    ("ym:ev:goal<goal_id>expense<currency>CRR", "number"),
]

_METRICS_FIELDS = prepare_fields_list(_RAW_METRICS_FIELDS)
print(_METRICS_FIELDS)

METRICS_FIELDS = _METRICS_FIELDS[0][0][1] + _METRICS_FIELDS[0][1] + _METRICS_FIELDS[1]

print(METRICS_FIELDS)
