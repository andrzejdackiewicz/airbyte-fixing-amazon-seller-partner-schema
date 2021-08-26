const DestinationIcon = ({
  color = "currentColor",
}: {
  color?: string;
}): JSX.Element => (
  <svg width="21" height="20" viewBox="0 0 21 20" fill="none">
    <path
      d="M9 9V6L14 10L9 14V11H0V9H9ZM1.458 13H3.582C4.28005 14.7191 5.55371 16.1422 7.18512 17.0259C8.81652 17.9097 10.7043 18.1991 12.5255 17.8447C14.3468 17.4904 15.9883 16.5142 17.1693 15.0832C18.3503 13.6523 18.9975 11.8554 19 10C19.001 8.14266 18.3558 6.34283 17.1749 4.90922C15.994 3.47561 14.3511 2.49756 12.528 2.14281C10.7048 1.78807 8.81505 2.07874 7.18278 2.96498C5.55051 3.85121 4.27747 5.27778 3.582 7H1.458C2.732 2.943 6.522 0 11 0C16.523 0 21 4.477 21 10C21 15.523 16.523 20 11 20C6.522 20 2.732 17.057 1.458 13Z"
      fill={color}
    />
  </svg>
);

export default DestinationIcon;
