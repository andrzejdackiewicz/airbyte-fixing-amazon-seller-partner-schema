const GoodIcon = ({ color = "currentColor" }: { color?: string }): JSX.Element => (
  <svg width="66" height="63" viewBox="0 0 66 63" fill="none">
    <path
      d="M40.8 23.0012H60C61.5913 23.0012 63.1174 23.6333 64.2426 24.7585C65.3679 25.8838 66 27.4099 66 29.0012V35.3132C66.0008 36.0973 65.8479 36.8739 65.55 37.5992L56.265 60.1442C56.0385 60.6938 55.6538 61.1638 55.1597 61.4944C54.6657 61.8251 54.0845 62.0014 53.49 62.0012H3C2.20435 62.0012 1.44129 61.6851 0.87868 61.1225C0.31607 60.5599 0 59.7968 0 59.0012V29.0012C0 28.2055 0.31607 27.4425 0.87868 26.8799C1.44129 26.3172 2.20435 26.0012 3 26.0012H13.446C13.9263 26.0013 14.3996 25.8861 14.8261 25.6653C15.2526 25.4445 15.6199 25.1244 15.897 24.7322L32.256 1.55118C32.4628 1.25811 32.7678 1.04886 33.1156 0.961347C33.4635 0.873836 33.8311 0.913865 34.152 1.07418L39.594 3.79518C41.1255 4.56066 42.349 5.82712 43.0612 7.38412C43.7734 8.94113 43.9314 10.6949 43.509 12.3542L40.8 23.0012ZM18 30.7652V56.0012H51.48L60 35.3132V29.0012H40.8C39.8862 29.001 38.9845 28.7922 38.1637 28.3906C37.3429 27.9889 36.6247 27.4051 36.0638 26.6836C35.503 25.9622 35.1143 25.1222 34.9275 24.2276C34.7408 23.3331 34.7607 22.4078 34.986 21.5222L37.695 10.8782C37.7798 10.5462 37.7483 10.1951 37.6059 9.88346C37.4634 9.57181 37.2185 9.31832 36.912 9.16518L34.929 8.17518L20.799 28.1912C20.049 29.2532 19.089 30.1232 18 30.7652ZM12 32.0012H6V56.0012H12V32.0012Z"
      fill={color}
    />
  </svg>
);

export default GoodIcon;
