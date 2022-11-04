import classNames from "classnames";
import React from "react";
import { ReflexContainer, ReflexElement, ReflexSplitter } from "react-reflex";

import { Heading } from "components/ui/Heading";

import styles from "./ResizablePanels.module.scss";

interface ResizablePanelsProps {
  className?: string;
  firstPanel: PanelProps;
  secondPanel: PanelProps;
  hideSecondPanel?: boolean;
}

interface PanelProps {
  children: React.ReactNode;
  minWidth: number;
  className?: string;
  startingFlex?: number;
  overlay?: Overlay;
}

interface Overlay {
  displayThreshold: number;
  header: string;
  rotation?: "clockwise" | "counter-clockwise";
}

interface PanelContainerProps {
  className?: string;
  dimensions?: {
    width: number;
    height: number;
  };
  overlay?: Overlay;
}

const PanelContainer: React.FC<React.PropsWithChildren<PanelContainerProps>> = ({
  children,
  className,
  dimensions,
  overlay,
}) => {
  const width = dimensions?.width ?? 0;

  return (
    <div className={classNames(className, styles.panelContainer)}>
      {overlay && width <= overlay.displayThreshold && (
        <div className={styles.lightOverlay}>
          <Heading
            as="h2"
            className={classNames(styles.rotatedHeader, {
              [styles.counterClockwise]: overlay?.rotation === "counter-clockwise",
            })}
          >
            {overlay.header}
          </Heading>
        </div>
      )}
      {children}
    </div>
  );
};

export const ResizablePanels: React.FC<ResizablePanelsProps> = ({
  className,
  firstPanel,
  secondPanel,
  hideSecondPanel = false,
}) => {
  return (
    <ReflexContainer className={className} orientation="vertical">
      <ReflexElement
        className={styles.panelStyle}
        propagateDimensions
        minSize={firstPanel.minWidth}
        flex={firstPanel.startingFlex}
      >
        <PanelContainer className={firstPanel.className} overlay={firstPanel.overlay}>
          {firstPanel.children}
        </PanelContainer>
      </ReflexElement>
      {/* NOTE: ReflexElement will not load its contents if wrapped in an empty jsx tag along with ReflexSplitter.  They must be evaluated/rendered separately. */}
      {!hideSecondPanel && (
        <ReflexSplitter className={styles.splitter}>
          <div className={styles.panelGrabber}>
            <div className={styles.grabberHandleIcon} />
          </div>
        </ReflexSplitter>
      )}
      {!hideSecondPanel && (
        <ReflexElement
          className={styles.panelStyle}
          propagateDimensions
          minSize={secondPanel.minWidth}
          flex={secondPanel.startingFlex}
        >
          <PanelContainer className={secondPanel.className} overlay={secondPanel.overlay}>
            {secondPanel.children}
          </PanelContainer>
        </ReflexElement>
      )}
    </ReflexContainer>
  );
};
