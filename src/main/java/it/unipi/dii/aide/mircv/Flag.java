package it.unipi.dii.aide.mircv;

public class Flag {

    private static boolean filter_flag;
    private static boolean compression_flag;


    public static boolean isFilterEnabled() { return filter_flag; }

    public static boolean isCompressionEnabled() { return compression_flag; }





    public static void setFilterFlag(boolean filter_flag) {
        Flag.filter_flag = filter_flag;
    }

    public static void setCompressionFlag(boolean compression_flag) {
        Flag.compression_flag = compression_flag;
    }

}
