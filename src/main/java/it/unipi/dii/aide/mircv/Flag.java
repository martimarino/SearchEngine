package it.unipi.dii.aide.mircv;

public final class Flag {

    private static boolean sws_flag = false;            // true = stop words removal enabled, false = stop words removal disabled
    private static boolean compression_flag = false;    // true = compression enabled, false = compression disabled
    private static boolean scoring_flag = false;        // true = scoring enable, false = scoring disable


    public static boolean isSwsEnabled() { return sws_flag; }

    public static boolean isCompressionEnabled() { return compression_flag; }

    public static boolean isScoringEnabled() { return scoring_flag; }



    public static void setSws(boolean sws_flag) { Flag.sws_flag = sws_flag; }

    public static void setCompression(boolean compression_flag) {
        Flag.compression_flag = compression_flag;
    }

    public static void setScoring(boolean scoring_flag) {
        Flag.scoring_flag = scoring_flag;
    }




}
