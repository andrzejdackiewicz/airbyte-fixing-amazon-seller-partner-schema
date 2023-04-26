interface Props {
  color?: string;
  width?: number;
  height?: number;
}

export const CrossIcon = ({ color = "#27272A", width = 20, height = 20 }: Props) => (
  <svg width={`${width}`} height={`${height}`} viewBox="0 0 26 26" fill="none" xmlns="http://www.w3.org/2000/svg">
    <circle cx="13" cy="13" r="13" fill={color} />
    <path
      fillRule="evenodd"
      clipRule="evenodd"
      d="M7.29289 7.29289C7.68342 6.90237 8.31658 6.90237 8.70711 7.29289L13 11.5858L17.2929 7.29289C17.6834 6.90237 18.3166 6.90237 18.7071 7.29289C19.0976 7.68342 19.0976 8.31658 18.7071 8.70711L14.4142 13L18.7071 17.2929C19.0976 17.6834 19.0976 18.3166 18.7071 18.7071C18.3166 19.0976 17.6834 19.0976 17.2929 18.7071L13 14.4142L8.70711 18.7071C8.31658 19.0976 7.68342 19.0976 7.29289 18.7071C6.90237 18.3166 6.90237 17.6834 7.29289 17.2929L11.5858 13L7.29289 8.70711C6.90237 8.31658 6.90237 7.68342 7.29289 7.29289Z"
      fill="white"
    />
  </svg>
);
