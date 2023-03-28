import React, { useState } from "react";
import { FormattedMessage } from "react-intl";
import styled from "styled-components";

import { Button } from "components";

import { useUser } from "core/AuthContext";
import { getRoleAgainstRoleNumber, ROLES } from "core/Constants/roles";
import { getPaymentStatus, PAYMENT_STATUS } from "core/Constants/statuses";
import useRouter from "hooks/useRouter";
import { RoutePaths } from "pages/routePaths";
import { useUserPlanDetail } from "services/payments/PaymentsService";

import styles from "../banners.module.scss";
import { UnauthorizedModal } from "./components/UnauthorizedModal";

interface IProps {
  onBillingPage: () => void;
}

const Banner = styled.div`
  width: 100%;
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: center;
`;

const Text = styled.div`
  font-weight: 500;
  font-size: 13px;
  line-height: 24px;
  display: flex;
  align-items: center;
  color: #ffffff;
  margin-right: 50px;
`;

export const UpgradePlanBanner: React.FC<IProps> = ({ onBillingPage }) => {
  const { user } = useUser();
  const { pathname } = useRouter();
  const userPlanDetail = useUserPlanDetail();
  const { expiresTime } = userPlanDetail;

  const remainingDaysForFreeTrial = (): number => {
    const currentDate: Date = new Date();
    const expiryDate: Date = new Date(expiresTime * 1000);
    const diff = expiryDate.getTime() - currentDate.getTime();
    const diffDays = Math.ceil(diff / (1000 * 60 * 60 * 24));
    return diffDays;
  };

  const isUpgradePlanBanner = (): boolean => {
    let showUpgradePlanBanner = false;
    if (getPaymentStatus(user.status) === PAYMENT_STATUS.Free_Trial) {
      if (!pathname.split("/").includes(RoutePaths.Payment)) {
        showUpgradePlanBanner = true;
      }
    }
    return showUpgradePlanBanner;
  };

  const [isAuthorized, setIsAuthorized] = useState<boolean>(false);

  const onUpgradePlan = () => {
    if (
      getRoleAgainstRoleNumber(user.role) === ROLES.Administrator_Owner ||
      getRoleAgainstRoleNumber(user.role) === ROLES.Administrator
    ) {
      onBillingPage();
    } else {
      setIsAuthorized(true);
    }
  };

  if (isUpgradePlanBanner()) {
    return (
      <>
        <Banner className={styles.banner}>
          <Text>
            <FormattedMessage
              id={
                remainingDaysForFreeTrial() >= 0 ? "upgrade.plan.trialPeriod.countdown" : "upgrade.plan.trialPeriod.end"
              }
              values={{ count: remainingDaysForFreeTrial() }}
            />
          </Text>
          <Button size="m" black onClick={() => onUpgradePlan()}>
            <FormattedMessage id="upgrade.plan.btn" />
          </Button>
        </Banner>
        {isAuthorized && <UnauthorizedModal onClose={() => setIsAuthorized(false)} />}
      </>
    );
  }
  return null;
};
