import React from "react";
import { QueryClient, QueryClientProvider } from "react-query";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      refetchOnReconnect: false,
      retry: 0,
      notifyOnChangePropsExclusions: ["isStale"],
    },
  },
});

const StoreProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
  <QueryClientProvider client={queryClient}>
    {/* <ReactQueryDevtools initialIsOpen={false} position="bottom-right" /> */}
    {children}
  </QueryClientProvider>
);

export { StoreProvider };
