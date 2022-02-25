import { AirbyteRequestService } from "core/request/AirbyteRequestService";
import { Operation } from "./operation";
import Status from "core/statuses";

class OperationService extends AirbyteRequestService {
  get url() {
    return "operations";
  }

  public async check(
    operation: Operation
  ): Promise<{ status: "succeeded" | "failed"; message: string }> {
    const rs = ((await this.fetch(
      `${this.url}/check`,
      operation.operatorConfiguration
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    )) as any) as {
      status: "succeeded" | "failed";
      message: string;
    };

    if (rs.status === Status.FAILED) {
      // TODO: place proper error
      throw new Error("failed");
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return rs as any;
  }
}

export { OperationService };
