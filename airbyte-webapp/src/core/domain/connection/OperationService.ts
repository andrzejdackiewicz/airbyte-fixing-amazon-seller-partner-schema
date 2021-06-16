import { AirbyteRequestService } from "core/request/AirbyteRequestService";
import { Operation } from "./operation";
import Status from "core/statuses";

class OperationService extends AirbyteRequestService {
  get url() {
    return "operations";
  }

  public async check(
    body: Operation
  ): Promise<{ status: "succeeded" | "failed"; message: string }> {
    const rs = ((await this.fetch(`${this.url}/check`, body)) as any) as {
      status: "succeeded" | "failed";
      message: string;
    };

    if (rs.status === Status.FAILED) {
      // TODO: place proper error
      throw new Error("failed");
    }

    return rs as any;
  }
}

export const operationService = new OperationService();
