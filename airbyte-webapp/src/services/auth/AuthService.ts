import { AirbyteRequestService } from "../../core/request/AirbyteRequestService";

export interface Signup {
  firstName: string;
  lastName: string;
  company: string;
  email: string;
  password: string;
  confirmPassword: string;
}

export interface Signin {
  email: string;
  password: string;
}

export class AuthService extends AirbyteRequestService {
  public async create(signup: Signup): Promise<Signup> {
    return new Promise((resolve, reject) => {
      this.fetch<Signup>(`/user/register`, signup)
        .then((res: any) => {
          resolve(res.data);
        })
        .catch((err: any) => {
          reject(err);
        });
    });
  }

  public async post(signin: Signin): Promise<Signin> {
    return new Promise((resolve, reject) => {
      this.fetch<Signin>(`/user/login`, signin)
        .then((res: any) => {
          resolve(res.data);
        })
        .catch((err: any) => {
          reject(err);
        });
    });
  }
}
