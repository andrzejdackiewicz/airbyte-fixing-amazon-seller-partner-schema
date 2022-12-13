import { flatten } from "flat";
import { FormikErrors, FormikTouched, useFormikContext } from "formik";
import intersection from "lodash/intersection";

import { BuilderView, useConnectorBuilderState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { BuilderFormValues, BuilderStream } from "./types";

export const useBuilderErrors = () => {
  const { touched, errors, validateForm, setFieldTouched } = useFormikContext<BuilderFormValues>();
  const { setSelectedView, setTestStreamIndex } = useConnectorBuilderState();

  const invalidViews = (
    ignoreUntouched: boolean,
    limitToViews?: BuilderView[],
    inputErrors?: FormikErrors<BuilderFormValues>
  ) => {
    const errorsToCheck = inputErrors !== undefined ? inputErrors : errors;
    console.log("errorsToCheck", errorsToCheck);

    const errorKeys = ignoreUntouched
      ? intersection(Object.keys(errorsToCheck), Object.keys(touched))
      : Object.keys(errorsToCheck);
    console.log("errorKeys", errorKeys);

    const invalidViews: BuilderView[] = [];

    if (errorKeys.includes("global")) {
      invalidViews.push("global");
    }

    if (errorKeys.includes("streams")) {
      const errorStreamNums = Object.keys(errorsToCheck.streams ?? {});

      if (ignoreUntouched) {
        // have to cast to allow us to index into the formik errors objects
        const streamsErrors = errorsToCheck.streams as Array<FormikErrors<BuilderStream>>;
        const streamsTouched = touched.streams as Array<FormikTouched<BuilderStream>>;
        // loop over each stream and find ones with fields that are both touched and erroring
        for (const streamNumString of errorStreamNums) {
          const streamNum = Number(streamNumString);
          const streamErrors = streamsErrors[streamNum];
          const streamTouched = streamsTouched[streamNum];
          const streamErrorKeys = Object.keys(flatten(streamErrors));
          const streamTouchedKeys = Object.keys(flatten(streamTouched));
          if (intersection(streamErrorKeys, streamTouchedKeys).length > 0) {
            invalidViews.push(streamNum);
          }
        }
      } else {
        invalidViews.push(...errorStreamNums.map((numString) => Number(numString)));
      }
    }

    return limitToViews === undefined ? invalidViews : intersection(invalidViews, limitToViews);
  };

  // Returns true if the global config fields or any stream config fields have errors in the provided formik errors, and false otherwise.
  // If limitToViews is provided, the error check is limited to only those views.
  const hasErrors = (ignoreUntouched: boolean, limitToViews?: BuilderView[]) => {
    return invalidViews(ignoreUntouched, limitToViews).length > 0;
  };

  const validateAndTouch = (callback: () => void, limitToViews?: BuilderView[]) => {
    validateForm().then((errors) => {
      for (const path of Object.keys(flatten(errors))) {
        setFieldTouched(path);
      }

      // If there are relevant errors, select the erroring view, prioritizing global
      // Otherwise, execute the callback.

      const invalidBuilderViews = invalidViews(false, limitToViews, errors);

      if (invalidBuilderViews.length > 0) {
        if (invalidBuilderViews.includes("global")) {
          setSelectedView("global");
        } else {
          setSelectedView(invalidBuilderViews[0]);
          setTestStreamIndex(invalidBuilderViews[0] as number);
        }
      } else {
        callback();
      }
    });
  };

  return { hasErrors, validateAndTouch };
};
