package it.unipi.dii.aide.mircv;

public class Flag {

    private static boolean sws_flag = false;
    private static boolean compression_flag = false;
    private static boolean scoring_flag = false;


    public static boolean isSwsEnabled() { return sws_flag; }

    public static boolean isCompressionEnabled() { return compression_flag; }

    public static boolean isScoringEnabled() { return scoring_flag; }



    public static void enableSws(boolean sws_flag) { Flag.sws_flag = sws_flag; }

    public static void enableCompression(boolean compression_flag) {
        Flag.compression_flag = compression_flag;
    }

    public static void enableScoring(boolean scoring_flag) {
        Flag.scoring_flag = scoring_flag;
    }




}
