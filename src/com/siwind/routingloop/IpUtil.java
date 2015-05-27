package com.siwind.routingloop;

public class IpUtil {

	/**
	 * 由IP地址和网络彥码，获得IP网段地址
	 * 
	 * @param ip
	 * @param masklen
	 * @return
	 */
	public static int getNetInt(int ip, int masklen) {
		int ipaddr = ip;
		if (masklen >= 0 && masklen <= 32) {// make a net mask !
			int mask = (masklen == 32) ? 0xffffffff : ((1 << masklen) -1 ) << (32 - masklen);
			//System.out.println(Integer.toBinaryString(mask));
			ipaddr &= mask;
		}
		return ipaddr;
	}

	/**
	 * 将字符串表示的ip地址转换为int表示.
	 * 
	 * @param ip
	 *            ip地址
	 * @return 以32位整数表示的ip地址
	 */
	public static int Ip2Int(String strIp) {
		String[] ss = strIp.split("\\.");
		if (ss.length != 4) {
			return 0;
		}
		byte[] bytes = new byte[4];
		for (int i = 0; i < bytes.length; i++) {
			bytes[i] = (byte) Integer.parseInt(ss[i]);
		}
		return byte2Int(bytes);
	}

	/**
	 * 将整数表示的ip地址转换为字符串表示.
	 * 
	 * @param ip
	 *            32位整数表示的ip地址
	 * @return 点分式表示的ip地址
	 */
	public static String Ip2Str(int intIp) {
		byte[] bytes = int2byte(intIp);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 4; i++) {
			sb.append(bytes[i] & 0xFF);
			if (i < 3) {
				sb.append(".");
			}
		}
		return sb.toString();
	}

	private static byte[] int2byte(int i) {
		byte[] bytes = new byte[4];
		bytes[3] = (byte) (0xff & i);
		bytes[2] = (byte) ((0xff00 & i) >> 8);
		bytes[1] = (byte) ((0xff0000 & i) >> 16);
		bytes[0] = (byte) ((0xff000000 & i) >> 24);
		return bytes;
	}

	private static int byte2Int(byte[] bytes) {
		int n = bytes[3] & 0xFF;
		n |= ((bytes[2] << 8) & 0xFF00);
		n |= ((bytes[1] << 16) & 0xFF0000);
		n |= ((bytes[0] << 24) & 0xFF000000);
		return n;
	}

	public static void main(String[] args) {
		System.out.println(Ip2Str(Ip2Int("192.168.0.0")));
		System.out.println(Ip2Str(Ip2Int("192.168.1.0")));
		System.out.println(Ip2Str(Ip2Int("192.168.2.0")));

		System.out.println(Ip2Str(getNetInt(Ip2Int("192.168.2.224"),32)));
		System.out.println(Ip2Str(getNetInt(Ip2Int("192.168.2.224"),24)));
		System.out.println(Ip2Str(getNetInt(Ip2Int("192.168.2.224"),16)));
		System.out.println(Ip2Str(getNetInt(Ip2Int("192.168.2.224"),8)));
		System.out.println(Ip2Str(getNetInt(Ip2Int("192.168.2.224"),4)));
		System.out.println(Ip2Str(getNetInt(Ip2Int("192.168.2.224"),1)));
		System.out.println(Ip2Str(getNetInt(Ip2Int("192.168.2.224"),0)));
	}
}
