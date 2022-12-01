import { ToastProps } from "components/ui/Toast";

export type Notification = ToastProps & { id: string | number; nonClosable?: boolean };

export interface NotificationServiceApi {
  addNotification: (notification: Notification) => void;
  deleteNotificationById: (notificationId: string | number) => void;
  clearAll: () => void;
}

export interface NotificationServiceState {
  notifications: Notification[];
}
