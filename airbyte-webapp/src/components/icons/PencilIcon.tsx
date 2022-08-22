interface PencilIconProps {
  color?: string;
}

export const PencilIcon = ({ color = "currentColor" }: PencilIconProps) => (
  <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
    <path
      d="M7.675 3.14356L10.8565 6.32581L3.4315 13.7501H0.25V10.5678L7.675 3.14281V3.14356ZM8.7355 2.08306L10.3262 0.491563C10.4669 0.350959 10.6576 0.271973 10.8565 0.271973C11.0554 0.271973 11.2461 0.350959 11.3868 0.491563L13.5085 2.61331C13.6491 2.75396 13.7281 2.94469 13.7281 3.14356C13.7281 3.34244 13.6491 3.53317 13.5085 3.67381L11.917 5.26456L8.7355 2.08306Z"
      fill={color}
    />
  </svg>
);
