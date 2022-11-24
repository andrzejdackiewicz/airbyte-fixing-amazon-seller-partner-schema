import { theme } from "theme";

interface Props {
  color?: string;
  width?: number;
  height?: number;
}

export const CalendarIcon = ({ color = theme.primaryColor, width = 18, height = 18 }: Props) => (
  <svg width={`${width}`} height={`${height}`} viewBox="0 0 29 29" fill="none" xmlns="http://www.w3.org/2000/svg">
    <rect x="2" y="11" width="24" height="15" fill="#EAE9FF" />
    <path
      d="M8.61931 0C9.30109 0 9.8564 0.553484 9.8564 1.23709V2.07098H18.1843V1.23709C18.1843 0.908994 18.3146 0.594335 18.5466 0.362335C18.7786 0.130336 19.0933 0 19.4214 0C19.7495 0 20.0642 0.130336 20.2962 0.362335C20.5282 0.594335 20.6585 0.908994 20.6585 1.23709V2.07098H25.4987C26.9026 2.07098 28.0407 3.20727 28.0407 4.61297V25.5427C28.0407 26.9466 26.9044 28.0847 25.4987 28.0847H2.54199C1.13812 28.0847 0 26.9484 0 25.5445V4.61297C0 3.2091 1.13629 2.07098 2.54199 2.07098H7.38039V1.23709C7.38039 0.553484 7.9357 0 8.61931 0ZM18.1861 4.45353H9.85457V6.63813C9.85457 6.80059 9.82257 6.96146 9.7604 7.11155C9.69823 7.26164 9.60711 7.39802 9.49223 7.51289C9.37736 7.62777 9.24098 7.71889 9.09089 7.78106C8.9408 7.84323 8.77994 7.87523 8.61748 7.87523C8.45502 7.87523 8.29416 7.84323 8.14407 7.78106C7.99398 7.71889 7.8576 7.62777 7.74273 7.51289C7.62785 7.39802 7.53673 7.26164 7.47456 7.11155C7.41239 6.96146 7.38039 6.80059 7.38039 6.63813V4.45353H2.54199C2.4997 4.45353 2.45915 4.47032 2.42924 4.50023C2.39934 4.53013 2.38254 4.57068 2.38254 4.61297V9.49719H25.6582V4.61481C25.6582 4.57252 25.6414 4.53196 25.6115 4.50206C25.5816 4.47216 25.541 4.45536 25.4987 4.45536H20.6603V6.63997C20.6603 6.96807 20.53 7.28272 20.298 7.51472C20.066 7.74672 19.7513 7.87706 19.4232 7.87706C19.0951 7.87706 18.7805 7.74672 18.5485 7.51472C18.3165 7.28272 18.1861 6.96807 18.1861 6.63997V4.45353ZM2.38254 11.8816V25.5427C2.38254 25.6307 2.45402 25.7022 2.54199 25.7022H25.4987C25.541 25.7022 25.5816 25.6854 25.6115 25.6555C25.6414 25.6256 25.6582 25.585 25.6582 25.5427V11.8816H2.38254Z"
      fill={color}
    />
    <path
      d="M13.9113 22.3387C13.5726 22.3387 13.2903 22.2258 13.0645 22L9.33871 18.3871C8.8871 17.9355 8.8871 17.2016 9.33871 16.75C9.79032 16.2984 10.5242 16.2984 11.0323 16.75L13.9113 19.5726L18.2581 15.3387C18.7097 14.8871 19.4435 14.8871 19.9516 15.3387C20.4032 15.7903 20.4032 16.5242 19.9516 16.9758L14.7581 22C14.5323 22.2258 14.25 22.3387 13.9113 22.3387Z"
      fill={color}
    />
  </svg>
);
