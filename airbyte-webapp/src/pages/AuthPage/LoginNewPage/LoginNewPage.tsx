import React, { useEffect, useCallback } from "react";
// import useRouter from "hooks/useRouter";
import { useSearchParams } from "react-router-dom";

import { LoadingPage } from "components";
// import MessageBox from "components/base/MessageBox";

import { useUser } from "core/AuthContext";
import { useAuthenticationService } from "services/auth/AuthSpecificationService";

const LoginNewPage: React.FC = () => {
  const [searchParams] = useSearchParams();
  const token = searchParams.get("token");
  const { setUser } = useUser();
  const authService = useAuthenticationService();
  // const [errorMessage, setErrorMessage] = useState<string>("");

  const getUserInfo = useCallback(() => {
    if (token) {
      authService
        .getUserInfo(token)
        .then((res: any) => {
          setUser?.({ ...res.data, token });
        })
        .catch((err) => {
          if (err.message) {
            // setErrorMessage(err.message);
          }
          //  setTimeout(() => {
          window.open(process.env.REACT_APP_WEBSITE_URL, "_self");
          // }, 1000);
        });
    }
  }, [authService, setUser, token]);

  useEffect(() => {
    if (token) {
      getUserInfo();
    }
  }, []);

  if (!token) {
    window.open(process.env.REACT_APP_WEBSITE_URL, "_self");
    return null;
  }

  return (
    <LoadingPage position="center" full>
      {/* {errorMessage && (
        <MessageBox
          type="error"
          message={errorMessage}
          onClose={() => {
            setErrorMessage("");
          }}
        />
      )} */}
    </LoadingPage>
  );
};

export default LoginNewPage;
