import { useEffect } from "react";
import { useIntl } from "react-intl";
import { fromEvent, interval, merge, throttleTime } from "rxjs";

import { useAppMonitoringService } from "./AppMonitoringService";
import { useNotificationService } from "./Notification";

interface BuildInfo {
  build: string;
}

const INTERVAL = 60_000;

/**
 * This hook sets up a check to /buildInfo.json, which is generated by the build system on every build
 * with a new hash. If it ever detects a new hash in it, we know that the Airbyte instance got updated
 * and show a notification to the user to reload the page.
 */
export const useBuildUpdateCheck = () => {
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const { trackError } = useAppMonitoringService();

  useEffect(() => {
    // Trigger the check every ${INTERVAL} milliseconds or whenever the window regains focus again
    const subscription = merge(interval(INTERVAL), fromEvent(window, "focus"))
      // Throttle it to maximum once every 10 seconds
      .pipe(throttleTime(10 * 1000))
      .subscribe(async () => {
        try {
          // Fetch the buildInfo.json file without using any browser cache
          const buildInfoResp = await fetch("/buildInfo.json", { cache: "no-store" });
          const buildInfo: BuildInfo = await buildInfoResp.json();

          if (buildInfo.build !== process.env.BUILD_HASH) {
            registerNotification({
              id: "webapp-updated",
              text: formatMessage({ id: "notifications.airbyteUpdated" }),
              nonClosable: true,
              actionBtnText: formatMessage({ id: "notifications.airbyteUpdated.reload" }),
              onAction: () => window.location.reload(),
            });
          }
        } catch (e) {
          // We ignore all errors from the build update check, since it's an optional check to
          // inform the user. We don't want to treat failed requests here as errors.
          trackError(e);
        }
      });

    return () => {
      subscription.unsubscribe();
    };
  }, [formatMessage, registerNotification, trackError]);
};
