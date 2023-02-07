import classNames from "classnames";
import React from "react";

import { isCloudApp } from "utils/app";

import styles from "./MainPageWithScroll.module.scss";

/**
 * @param headTitle the title shown in the browser toolbar
 * @param pageTitle the title shown on the page
 */
interface MainPageWithScrollProps {
  headTitle?: React.ReactNode;
  pageTitle?: React.ReactNode;
  children?: React.ReactNode;
  softScrollEdge?: boolean;
}

export const MainPageWithScroll: React.FC<MainPageWithScrollProps> = ({
  headTitle,
  pageTitle,
  softScrollEdge = true,
  children,
}) => {
  return (
    <>
      {headTitle}
      <div className={styles.container}>
        <div>{pageTitle}</div>
        <div
          className={classNames(styles.contentContainer, {
            [styles.softScrollEdge]: softScrollEdge,
          })}
        >
          <div className={styles.contentScroll}>
            <div
              className={classNames(styles.content, {
                [styles.cloud]: isCloudApp(),
              })}
            >
              {children}
            </div>
          </div>
          {softScrollEdge && <div className={styles.blur} aria-hidden="true" />}
        </div>
      </div>
    </>
  );
};
