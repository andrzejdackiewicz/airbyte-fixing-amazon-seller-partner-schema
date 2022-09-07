import { fireEvent, render, waitFor } from "@testing-library/react";
import { Formik } from "formik";
import { IntlProvider } from "react-intl";

import { ReleaseStage } from "core/request/AirbyteClient";
import en from "locales/en.json";

import { StartWithDestination, StartWithDestinationProps } from "./StartWithDestination";

const renderStartWithDestination = (props: StartWithDestinationProps) =>
  render(
    <Formik initialValues={{}} onSubmit={() => {}}>
      <IntlProvider locale="en" messages={en}>
        <StartWithDestination {...props} />
      </IntlProvider>
    </Formik>
  );

const destination = {
  destinationDefinitionId: "8b746512-8c2e-6ac1-4adc-b59faafd473c",
  name: "MongoDB",
  icon: '<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">\n<svg version="1.1" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" preserveAspectRatio="xMidYMid meet" viewBox="0 0 640 640" width="640" height="640"><defs><path d="M454.48 258.27C421.69 113.73 344.25 66.21 335.9 48.07C328.85 36.65 322.83 24.62 317.93 12.13C317.72 11.55 317.48 10.98 317.21 10.43C317.65 22.19 312.69 33.5 303.74 41.15C284.96 55.79 188.75 136.46 180.85 300.76C173.57 453.48 291.52 545.1 307.51 556.78C307.69 556.91 309.13 557.91 309.31 558.04C313.89 547.62 318.29 517.71 321.7 476.65C322.42 509.71 327.18 545.19 343.62 552.65C343.71 552.59 344.43 552.08 344.52 552.02C351.58 547.01 358.3 541.55 364.64 535.67C404.62 498.84 480.08 407.66 454.48 258.27" id="b3m9u0RT9j"></path><path d="M321.43 477.01C318.2 517.98 313.53 547.8 309.04 557.86C309.04 557.86 314.07 593.79 318.02 632.24C319.27 632.24 329.26 632.24 330.51 632.24C333.44 605.49 337.82 578.92 343.62 552.65C327.36 545.1 322.6 509.71 321.88 476.65" id="aevBvhJN5"></path><path d="M454.48 258.27C421.69 113.73 344.25 66.21 335.9 48.07C328.81 36.67 322.79 24.64 317.93 12.13C325.66 42.41 324.4 284.14 325.21 313.07C326.92 367.62 325.75 422.22 321.7 476.65C322.42 509.71 327.18 545.19 343.53 552.74C343.63 552.67 344.42 552.09 344.52 552.02C351.58 547.06 358.31 541.62 364.64 535.76C364.72 535.7 365.29 535.2 365.36 535.13C404.53 499.2 480.08 407.66 454.48 258.27" id="akHagKvoh"></path></defs><g><g><g><use xlink:href="#b3m9u0RT9j" opacity="1" fill="#12a950" fill-opacity="1"></use><g><use xlink:href="#b3m9u0RT9j" opacity="1" fill-opacity="0" stroke="#000000" stroke-width="1" stroke-opacity="0"></use></g></g><g><use xlink:href="#aevBvhJN5" opacity="1" fill="#b8c5c2" fill-opacity="1"></use><g><use xlink:href="#aevBvhJN5" opacity="1" fill-opacity="0" stroke="#000000" stroke-width="1" stroke-opacity="0"></use></g></g><g><use xlink:href="#akHagKvoh" opacity="1" fill="#13994f" fill-opacity="1"></use><g><use xlink:href="#akHagKvoh" opacity="1" fill-opacity="0" stroke="#000000" stroke-width="1" stroke-opacity="0"></use></g></g></g></g></svg>',
  releaseStage: ReleaseStage.alpha,
};

describe("<StartWithDestinations />", () => {
  test("should renders without crash with provided props", () => {
    const { asFragment } = renderStartWithDestination({ destination, onDestinationSelect: () => {} });

    expect(asFragment()).toMatchSnapshot();
  });

  test("should call provided handler with right params", async () => {
    const handler = jest.fn();
    const { getByText } = renderStartWithDestination({ destination, onDestinationSelect: handler });

    fireEvent.click(getByText("Start with MongoDB"));
    await waitFor(() => {
      expect(handler).toHaveBeenCalledTimes(1);
      expect(handler).toHaveBeenCalledWith("8b746512-8c2e-6ac1-4adc-b59faafd473c");
    });
  });
});
