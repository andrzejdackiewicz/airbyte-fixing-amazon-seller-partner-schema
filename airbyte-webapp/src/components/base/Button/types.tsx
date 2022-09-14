import React from "react";

type ButtonSize = "xs" | "sm" | "lg";
type ButtonVariant = "primary" | "secondary" | "danger" | "light";

export interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  clickable?: boolean;
  customStyles?: string;
  full?: boolean;
  icon?: React.ReactElement;
  iconPosition?: "left" | "right";
  isLoading?: boolean;
  size?: ButtonSize;
  variant?: ButtonVariant;
  wasActive?: boolean;
  width?: number;
}
