# utl-select-high-payment-periods-and-generating-code-with-do_over-and-dosubl

    Select high payment periods and generating code with do_over and dosubl;

         Two Solutions

             a. DO_OVER
             b. DOSUBL  (provides error checking and a log dataset)
             c. Merge
                Keintz, Mark
                mkeintz@outlook.com
             d. More rigorous
                Bartosz Jablonski
                yabwon@gmail.com

    Github
    https://tinyurl.com/4et8bmvw
    https://github.com/rogerjdeangelis/utl-select-high-payment-periods-and-generating-code-with-do_over-and-dosubl

    Other dosubl repos
    https://github.com/rogerjdeangelis?tab=repositories&q=dosubl&type=&language=&sort=

     As a side note the SAS SPDE engine can index between logic?

     Problem:

      Given meta data with account number and start and endates select
      records from tha ccunt master that match the account and time period in the
      meta data.


    *_                   _
    (_)_ __  _ __  _   _| |_
    | | '_ \| '_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    ;

    * I would be afraid of having a large number of where ckauses
      compilers tend work well with reoeated clgicals ;

    data havDte ;
      retain act;
      informat beg end date10.;
        format beg end date10.;
      input act beg end;
    CARDS4;
    1 01JAN2021 03JAN2021
    2 08jan2021 09JAN2021
    3 11JAN2021 13JAN2021
    4 11MAR2021 13MAR2021
    ;;;;
    run;quit;

    /*
    HAVDTE total obs=3

    Obs    ACT       BEG          END

     1      1     01JAN2021    03JAN2021
     2      2     08JAN2021    09JAN2021
     3      3     11JAN2021    13JAN2021
     4      4     11MAR2021    13MAR2021
    */

    data havMth;
      format act 1. day date9.;
      do day='01JAN2021'd to '15JAN2021'd;
          paymnt = int(100*uniform(1234));
          act=mod(day-1,3)+1 ;
          output;
      end;
    run;quit;

                                 |
    HAVMTH total obs=15          | RULES (Select these records)
                                 |
     ACT       DAY       PAYMNT  | PAYMNT
                                 |
      3     01JAN2021      24    |
      1     02JAN2021       8    |   8   act = 1 and day between 01JAN2021 and 03JAN2021
      2     03JAN2021      38    |
      3     04JAN2021       9    |
      1     05JAN2021      25    |
      2     06JAN2021       8    |
      3     07JAN2021       3    |
      1     08JAN2021      10    |
      2     09JAN2021      44    |  44   act = 2 and day between 08JAN2021 and 09JAN2021
      3     10JAN2021      14    |
      1     11JAN2021       3    |
      2     12JAN2021      45    |
      3     13JAN2021       8    |   4   act = 3 and day between 11JAN2021 and 13JAN2021
      1     14JAN2021      90    |
      2     15JAN2021      96    |


    *
     _ __  _ __ ___   ___ ___  ___ ___
    | '_ \| '__/ _ \ / __/ _ \/ __/ __|
    | |_) | | | (_) | (_|  __/\__ \__ \
    | .__/|_|  \___/ \___\___||___/___/
    |_|            _
      __ _      __| | ___     _____   _____ _ __
     / _` |    / _` |/ _ \   / _ \ \ / / _ \ '__|
    | (_| |_  | (_| | (_) | | (_) \ V /  __/ |
     \__,_(_)  \__,_|\___/___\___/ \_/ \___|_|
                        |_____|
    ;

    %array(_act,data=havDte,var=act);
    %array(_beg,data=havDte,var=beg);
    %array(_end,data=havDte,var=end);

    proc sql;
     create
         table want as
     select
         day
        ,paymnt
     from
        havMth
     where
        %do_over(_act _beg _end,phrase=%str(
           act = ?_act and day between ?_beg and ?_end),between=%str(or)
        )
    ;quit;

    %arraydelete(_act);
    %arraydelete(_beg);
    %arraydelete(_end);

    * or if you want the generated text;

    %put %do_over(_act _beg _end,phrase=%str(
           act = ?_act and day between ?_beg and ?_end),between=%str(or)
    );

    /*
    act = 1 and day between 01JAN2021 and 03JAN2021 or
    act = 2 and day between 08JAN2021 and 09JAN2021 or
    act = 3 and day between 11JAN2021 and 13JAN2021
    */


    *_            _                 _     _
    | |__      __| | ___  ___ _   _| |__ | |
    | '_ \    / _` |/ _ \/ __| | | | '_ \| |
    | |_) |  | (_| | (_) \__ \ |_| | |_) | |
    |_.__(_)  \__,_|\___/|___/\__,_|_.__/|_|

    ;

    data log;

     * data set to append seleted records;
     if _n_=0 then do;
        %dosubl('
           proc datasets lib=work nolist;
             delete want;
           run;quit;
        ');
     end;

     set havDte;

     * pass to disubl routine;

     call symputx('_act',act);
     call symputx('_beg',beg);
     call symputx('_end',end);

     rc=dosubl('
        proc sql noprint;
            create
               table _add as
            select
               *
            from
               havMth
            where
               act=&_act and day between &_beg and &_end
        ;quit;
        %let cc=&sqlobs;
        %if &sqlobs ne 0 %then %do;
            proc append base=want data=havMth(where=(act=&_act and day between &_beg and &_end ));
            run;quit;
        %end;
        ');

     if symgetn('cc') ne 0 then status="Found a Record    ";
     else status="No record found";

    run;quit;

    *            _               _
      ___  _   _| |_ _ __  _   _| |_
     / _ \| | | | __| '_ \| | | | __|
    | (_) | |_| | |_| |_) | |_| | |_
     \___/ \__,_|\__| .__/ \__,_|\__|
                   _|_|
      __ _      __| | ___     _____   _____ _ __
     / _` |    / _` |/ _ \   / _ \ \ / / _ \ '__|
    | (_| |_  | (_| | (_) | | (_) \ V /  __/ |
     \__,_(_)  \__,_|\___/___\___/ \_/ \___|_|
                        |_____|
    ;

    WANT total obs=3

    Obs       DAY       PAYMNT

     1     02JAN2021       8
     2     09JAN2021      44
     3     13JAN2021       8

    *_            _                 _     _
    | |__      __| | ___  ___ _   _| |__ | |
    | '_ \    / _` |/ _ \/ __| | | | '_ \| |
    | |_) |  | (_| | (_) \__ \ |_| | |_) | |
    |_.__(_)  \__,_|\___/|___/\__,_|_.__/|_|

    ;

    Dosubl has th bonus of a log

    LOG total obs=4

    Obs    ACT       BEG          END       RC        STATUS

     1      1     01JAN2021    03JAN2021     0    Found a Record
     2      2     08JAN2021    09JAN2021     0    Found a Record
     3      3     11JAN2021    13JAN2021     0    Found a Record
     4      4     11MAR2021    13MAR2021     0    No record found


    WANT total obs=3

    Obs       DAY       PAYMNT

     1     02JAN2021       8
     2     09JAN2021      44
     3     13JAN2021       8

    *
      ___     _ __ ___   ___ _ __ __ _  ___
     / __|   | '_ ` _ \ / _ \ '__/ _` |/ _ \
    | (__ _  | | | | | |  __/ | | (_| |  __/
     \___(_) |_| |_| |_|\___|_|  \__, |\___|
                                 |___/
    ;

    Keintz, Mark
    mkeintz@outlook.com

    Anytime you have a data set with each obs providing a date range,
    to be matched somehow to another dataset in the form of a time
    series, a handy tool is to interleave, using the date range dataset
    twice (once for BEG, once for END) and the time series once.  It’s pure KISS.

    Of course this assumes that each dataset is sorted chronologically,
    and that the date range dataset does not have overlapping ranges:


    data want;

      set havdte (keep=beg rename=(beg=day) in=inbeg)

          havmth

          havdte (keep=end rename=(end=day) in=inend) ;

      by day;

      if inbeg then set havdte (rename=(act=target));

      else if inend then call missing(target);

      else if act=target then output;

    run;


    OK, I used HAVDTE three times, not twice, mostly to
    avoid specifying a retain for the TARGET variable.

    *    _               _               _
      __| |    _ __ ___ | |__  _   _ ___| |_
     / _` |   | '__/ _ \| '_ \| | | / __| __|
    | (_| |_  | | | (_) | |_) | |_| \__ \ |_
     \__,_(_) |_|  \___/|_.__/ \__,_|___/\__|

    ;

    Bartosz Jablonski
    yabwon@gmail.com

    I think that with small modifications the condition "the date range dataset
    does not have overlapping ranges" can be relaxed (up to some point at least)

    All the best
    Bart

    /*CODE*/
    /*- time periods -*/
    data havDte ;
      retain act;
      informat beg end date10.;
        format beg end date10.;
      input act beg end;
      /*- set-up periods names ---*/
      if lag(act) ne act then n = 32;
      n + 1; drop n;
      period = byte(n); /* works with up to 223 overlapping periods */
      /*--------------------------*/
    CARDS4;
    1 01JAN2021 03JAN2021
    1 03JAN2021 05JAN2021
    1 05JAN2021 07JAN2021
    2 05jan2021 10JAN2021
    2 06jan2021 09JAN2021
    2 07jan2021 08JAN2021
    3 11JAN2021 13JAN2021
    4 11MAR2021 13MAR2021
    ;;;;
    run;
    proc print;
    run;

    /*- data -*/
    data havMth;
      format act 1. day date9.;
      do act=1,2,3;
        do day='01JAN2021'd to '15JAN2021'd;
          paymnt = int(100*uniform(1234));
          output;
        end;
      end;
    run;
    proc print;
    run;



    /*- begins -*/
    proc sort data=havDte(drop=end) out=havDteB;
      by act beg;
    run;
    proc print;
    run;

    /*- ends -*/
    proc sort data=havDte(drop=beg) out=havDteE;
      by act end;
    run;
    proc print;
    run;

    /*- sorting -*/
    proc sort data = havMth;
      by act day;
    run;

    /*- processing -*/
    data want ;
      set havDteB(rename=(beg=day) in=b)
          havMth
          havDteE(rename=(end=day) in=e)
          ;
      by act day;

      trigger + b - e;

      /*- variable for overlapping periods -*/
      retain overlaps;
      length overlaps $ 223;
      /*- add new period to the list of overlapping -*/
      if b then overlaps = cats(overlaps,period);


      if trigger and paymnt >.z then
        do;
          /*- outputs for overlapping periods -*/
          do i = 1 to trigger;
            prd = rank(char(overlaps,i))-32;
            output;
          end;
        end;

      /*- remove period from the list of overlapping -*/
      if e then overlaps = compress(overlaps, period);

      if last.act;
      trigger = 0;
      overlaps = "";
      keep act day paymnt prd;
    run;
    /*- result -*/
    options ps=max;
    proc print data=want;
    run;

    /*OUTPUT*/
    Obs    act           day    paymnt    prd

      1     1      01JAN2021      24       1
      2     1      02JAN2021       8       1
      3     1      03JAN2021      38       1
      4     1      03JAN2021      38       2
      5     1      04JAN2021       9       2
      6     1      05JAN2021      25       2
      7     1      05JAN2021      25       3
      8     1      06JAN2021       8       3
      9     1      07JAN2021       3       3
     10     2      05JAN2021      51       1
     11     2      06JAN2021      28       1
     12     2      06JAN2021      28       2
     13     2      07JAN2021      35       1
     14     2      07JAN2021      35       2
     15     2      07JAN2021      35       3
     16     2      08JAN2021      25       1
     17     2      08JAN2021      25       2
     18     2      08JAN2021      25       3
     19     2      09JAN2021      43       1
     20     2      09JAN2021      43       2
     21     2      10JAN2021      12       1
     22     3      11JAN2021      49       1
     23     3      12JAN2021      94       1
     24     3      13JAN2021      70       1
