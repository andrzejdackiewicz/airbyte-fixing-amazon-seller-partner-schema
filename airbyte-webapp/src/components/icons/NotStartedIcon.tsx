interface Props {
  width?: number;
  height?: number;
}
export const NotStartedIcon = ({ width = 24, height = 24 }: Props) => (
  <svg xmlns="http://www.w3.org/2000/svg" width={`${width}`} height={`${height}`} viewBox="0 0 24 24" fill="none">
    <path
      d="M12 22C17.5228 22 22 17.5228 22 12C22 6.47715 17.5228 2 12 2C6.47715 2 2 6.47715 2 12C2 17.5228 6.47715 22 12 22Z"
      stroke="#AAAAAA"
      stroke-linecap="round"
      stroke-linejoin="round"
    />
    <path d="M15 9H9V15H15V9Z" stroke="#AAAAAA" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
  </svg>
);
