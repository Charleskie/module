import java.util.Scanner;

public class testSibat{

    public static boolean process(String s) {

        if(s == null || s.length() == 0)
            return false;

        int len = s.length();
        boolean[][] dp = new boolean[len][len];
        for(int i = 0; i < len; i++) {
            if(s.charAt(i) == '0' || s.charAt(i) == '1') {
                dp[i][i] = true;
            }
        }

        //区间的长度
        for(int size = 1; size < len; size++) {
            for(int i = 0; i < len-size; i++) {
                int j = i + size;
                if(s.charAt(i) == '(' && s.charAt(j) == ')' && dp[i+1][j-1]) {
                    dp[i][j] = true;
                }
                if(s.charAt(j) == '*' && dp[i][j-1]) {
                    dp[i][j] = true;
                }
                if(dp[i][j])
                    continue;
                for(int k = i; k < j; k++) {
                    if(dp[i][k] && dp[k+1][j])
                        dp[i][j] = true;
                    if(s.charAt(k) == '|' && k > 0 && dp[i][k-1] && dp[k+1][j])
                        dp[i][j] = true;
                    if(dp[i][j])
                        break;
                }
            }
        }

//        for(int i = 0; i < len; i++) {
//            for(int j = 0; j < len; j++) {
//                System.out.print(dp[i][j] + "   ");
//            }
//            System.out.println();
//        }
        return dp[0][len-1];


    }


    public static void main(String[] args) {
//        System.out.println(process("010101101*"));
//        System.out.println(process("(11|0*)*"));
//        System.out.println(process(")*111"));

        Scanner sc = new Scanner(System.in);
        String line = sc.nextLine();
        if(process(line)){
            System.out.println("yes");
        }else System.out.println("no");

    }
}
