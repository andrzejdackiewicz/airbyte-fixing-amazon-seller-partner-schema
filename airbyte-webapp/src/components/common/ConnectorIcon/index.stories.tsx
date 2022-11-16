import { ComponentStory, ComponentMeta } from "@storybook/react";
import classNames from "classnames";

import { ConnectorCard } from "components/ConnectorCard";

import { ConnectorIcon } from "./ConnectorIcon";
import styles from "./ConnectorIcon.story.module.scss";

export default {
  title: "Common/ConnectorIcon",
  component: ConnectorIcon,
} as ComponentMeta<typeof ConnectorIcon>;

const Template: ComponentStory<typeof ConnectorIcon> = (args) => <ConnectorIcon {...args} />;

export const Primary = Template.bind({});
Primary.args = {
  icon: `<svg width="34" height="35" viewBox="0 0 34 35" fill="none" xmlns="http://www.w3.org/2000/svg">
  <path fill-rule="evenodd" clip-rule="evenodd" d="M11.542 4.24644C15.2862 0.0397129 21.4865 -1.19433 26.584 1.23386C33.3566 4.4604 35.8274 12.6796 32.1401 19.0197L23.8451 33.2666C23.3815 34.0627 22.6187 34.6436 21.7242 34.8817C20.8297 35.1197 19.8766 34.9956 19.0742 34.5364L29.1168 17.2847C31.7921 12.6832 30.0027 6.71867 25.0925 4.37026C21.4083 2.6083 16.9045 3.4885 14.1828 6.51869C12.6815 8.18218 11.8386 10.3299 11.8105 12.5635C11.7824 14.7972 12.571 16.9651 14.0299 18.6654C14.2921 18.9705 14.5743 19.2581 14.8746 19.5264L9.01176 29.6158C8.78249 30.0102 8.47724 30.356 8.11342 30.6332C7.74961 30.9105 7.33436 31.1139 6.89139 31.2318C6.44842 31.3497 5.9864 31.3798 5.53171 31.3204C5.07702 31.261 4.63857 31.1133 4.24138 30.8856L10.6061 19.932C9.69184 18.6232 9.03531 17.1543 8.67109 15.603L4.7709 22.3286C4.30728 23.1247 3.54448 23.7056 2.64998 23.9437C1.75548 24.1817 0.802394 24.0576 0 23.5984L10.0859 6.24982C10.5057 5.53738 10.9933 4.86652 11.542 4.24644ZM23.1831 9.76855C25.6121 11.1616 26.451 14.2597 25.0455 16.6708L15.3738 33.2646C14.9102 34.0607 14.1474 34.6415 13.2529 34.8796C12.3584 35.1177 11.4053 34.9935 10.6029 34.5344L19.5834 19.0855C18.8628 18.9349 18.1838 18.6308 17.5931 18.1942C17.0024 17.7575 16.5142 17.1988 16.1622 16.5566C15.8101 15.9143 15.6026 15.2039 15.554 14.4742C15.5054 13.7446 15.6168 13.0132 15.8806 12.3305C16.1444 11.6478 16.5542 11.03 17.0818 10.5199C17.6094 10.0098 18.2422 9.61953 18.9365 9.37596C19.6308 9.13238 20.37 9.04134 21.1032 9.10912C21.8364 9.1769 22.546 9.40188 23.1831 9.76855ZM19.6651 12.8871C19.4989 13.0138 19.3594 13.1718 19.2547 13.352H19.2542C19.0964 13.6235 19.0233 13.9353 19.0439 14.2481C19.0646 14.5608 19.1782 14.8605 19.3704 15.1091C19.5626 15.3577 19.8246 15.5442 20.1235 15.6449C20.4224 15.7457 20.7446 15.7561 21.0495 15.675C21.3544 15.5938 21.6282 15.4248 21.8362 15.1891C22.0443 14.9534 22.1774 14.6618 22.2186 14.3511C22.2598 14.0403 22.2073 13.7244 22.0677 13.4433C21.9281 13.1622 21.7077 12.9285 21.4344 12.7718C21.2529 12.6677 21.0526 12.6002 20.8448 12.573C20.637 12.5459 20.4259 12.5596 20.2235 12.6135C20.0211 12.6674 19.8314 12.7603 19.6651 12.8871Z" fill="#615EFF"></path>
</svg>`,
};

export const ValidateIcons = ({ icon }: { icon: string }) => (
  <div className={styles.wrapper}>
    {/* Show in context of table */}
    <Template icon={icon} className={classNames(styles.container, styles.small)} />
    <Template icon={icon} className={styles.container} />
    <Template icon={icon} className={classNames(styles.container, styles.large)} />
    <div>The following icon should have a pink background:</div>
    <Template icon={icon} className={classNames(styles.container, styles.huge)} />
    <div>ConnectorCard:</div>
    <ConnectorCard
      connectionName="Connection Name"
      icon={icon}
      releaseStage="generally_available"
      connectorName="ConnectorName"
    />
  </div>
);
ValidateIcons.parameters = {
  controls: { exclude: ["className"] },
};
ValidateIcons.args = {
  icon: `<svg width="34" height="35" viewBox="0 0 34 35" fill="none" xmlns="http://www.w3.org/2000/svg">
  <path fill-rule="evenodd" clip-rule="evenodd" d="M11.542 4.24644C15.2862 0.0397129 21.4865 -1.19433 26.584 1.23386C33.3566 4.4604 35.8274 12.6796 32.1401 19.0197L23.8451 33.2666C23.3815 34.0627 22.6187 34.6436 21.7242 34.8817C20.8297 35.1197 19.8766 34.9956 19.0742 34.5364L29.1168 17.2847C31.7921 12.6832 30.0027 6.71867 25.0925 4.37026C21.4083 2.6083 16.9045 3.4885 14.1828 6.51869C12.6815 8.18218 11.8386 10.3299 11.8105 12.5635C11.7824 14.7972 12.571 16.9651 14.0299 18.6654C14.2921 18.9705 14.5743 19.2581 14.8746 19.5264L9.01176 29.6158C8.78249 30.0102 8.47724 30.356 8.11342 30.6332C7.74961 30.9105 7.33436 31.1139 6.89139 31.2318C6.44842 31.3497 5.9864 31.3798 5.53171 31.3204C5.07702 31.261 4.63857 31.1133 4.24138 30.8856L10.6061 19.932C9.69184 18.6232 9.03531 17.1543 8.67109 15.603L4.7709 22.3286C4.30728 23.1247 3.54448 23.7056 2.64998 23.9437C1.75548 24.1817 0.802394 24.0576 0 23.5984L10.0859 6.24982C10.5057 5.53738 10.9933 4.86652 11.542 4.24644ZM23.1831 9.76855C25.6121 11.1616 26.451 14.2597 25.0455 16.6708L15.3738 33.2646C14.9102 34.0607 14.1474 34.6415 13.2529 34.8796C12.3584 35.1177 11.4053 34.9935 10.6029 34.5344L19.5834 19.0855C18.8628 18.9349 18.1838 18.6308 17.5931 18.1942C17.0024 17.7575 16.5142 17.1988 16.1622 16.5566C15.8101 15.9143 15.6026 15.2039 15.554 14.4742C15.5054 13.7446 15.6168 13.0132 15.8806 12.3305C16.1444 11.6478 16.5542 11.03 17.0818 10.5199C17.6094 10.0098 18.2422 9.61953 18.9365 9.37596C19.6308 9.13238 20.37 9.04134 21.1032 9.10912C21.8364 9.1769 22.546 9.40188 23.1831 9.76855ZM19.6651 12.8871C19.4989 13.0138 19.3594 13.1718 19.2547 13.352H19.2542C19.0964 13.6235 19.0233 13.9353 19.0439 14.2481C19.0646 14.5608 19.1782 14.8605 19.3704 15.1091C19.5626 15.3577 19.8246 15.5442 20.1235 15.6449C20.4224 15.7457 20.7446 15.7561 21.0495 15.675C21.3544 15.5938 21.6282 15.4248 21.8362 15.1891C22.0443 14.9534 22.1774 14.6618 22.2186 14.3511C22.2598 14.0403 22.2073 13.7244 22.0677 13.4433C21.9281 13.1622 21.7077 12.9285 21.4344 12.7718C21.2529 12.6677 21.0526 12.6002 20.8448 12.573C20.637 12.5459 20.4259 12.5596 20.2235 12.6135C20.0211 12.6674 19.8314 12.7603 19.6651 12.8871Z" fill="#615EFF"></path>
</svg>`,
};
