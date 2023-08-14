package it.unipi.dii.aide.mircv;

public class Options {

    private static boolean filter_option;
    private static boolean compression_option;


    public static boolean isFilterEnabled() { return filter_option; }

    public static boolean isCompressionEnabled() { return compression_option; }





    public static void setFilterOption(boolean filter_option) {
        Options.filter_option = filter_option;
    }

    public static void setCompressionOption(boolean compression_option) {
        Options.compression_option = compression_option;
    }

}
