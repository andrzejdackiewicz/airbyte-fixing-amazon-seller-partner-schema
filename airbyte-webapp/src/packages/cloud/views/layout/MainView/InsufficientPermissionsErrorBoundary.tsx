import React from "react";

import { CommonRequestError } from "core/request/CommonRequestError";

type BoundaryState = { hasError: boolean; message?: React.ReactNode | null };

const initialState: BoundaryState = {
  hasError: false,
  message: null,
};

export class InsufficientPermissionsErrorBoundary extends React.Component<
  { errorComponent: React.ReactElement },
  BoundaryState
> {
  static getDerivedStateFromError(error: CommonRequestError): BoundaryState {
    if (error.message.startsWith("Insufficient permissions")) {
      return { hasError: true, message: error.message };
    } else {
      throw error;
    }
  }

  state = initialState;

  reset = (): void => {
    this.setState(initialState);
  };

  render(): React.ReactNode {
    return this.state.hasError
      ? React.cloneElement(this.props.errorComponent, {
          message: this.state.message,
          onReset: this.reset,
        })
      : this.props.children;
  }
}
