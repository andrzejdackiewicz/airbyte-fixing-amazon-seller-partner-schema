import React from "react";
import styled from "styled-components";

const RadioButtonInput = styled.input`
  opacity: 0;
  width: 0;
  height: 0;
  margin: 0;
`;

const Check = styled.div<{ checked?: boolean }>`
  height: 100%;
  width: 100%;
  border-radius: 50%;
  background: ${({ theme, checked }) => (checked ? theme.whiteColor : theme.greyColor20)};
`;

const RadioButtonContainer = styled.label<{ checked?: boolean }>`
  height: 18px;
  width: 18px;
  background: ${({ theme, checked }) => (checked ? theme.primaryColor : theme.whiteColor)};
  border: 1px solid ${({ theme, checked }) => (checked ? theme.primaryColor : theme.greyColor20)};
  color: ${({ theme }) => theme.whiteColor};
  text-align: center;
  border-radius: 50%;
  display: inline-block;
  padding: 4px;
  cursor: pointer;
`;

const RadioButton: React.FC<React.InputHTMLAttributes<HTMLInputElement>> = (props) => {
  return (
    <RadioButtonContainer
      className={props.className}
      onClick={(event: React.SyntheticEvent) => event.stopPropagation()}
      checked={props.checked}
    >
      <Check checked={props.checked} />
      <RadioButtonInput {...props} type="radio" />
    </RadioButtonContainer>
  );
};

export default RadioButton;
