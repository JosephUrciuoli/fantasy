import requests
import pandas as pd
import re
import json
import math

BOOKS = [
    "bet365",
    "betcris",
    "betmgm",
    "betonline",
    "betway",
    "bovada",
    "corale",
    "circa",
    "draftkings",
    "fanduel",
    "pinnacle",
    "skybet",
    "sportsbook",
    "unibet",
    "williamhill",
]

MARKETS = ["win", "top_5", "top_10", "top_20", "make_cut", "mc"]

SITES = ["draftkings", "fanduel"]


def _get_events_list() -> list:
    """Gets the events from Data Golf."""
    url = "https://feeds.datagolf.com/historical-odds/event-list?tour=pga&key=9e89123f99ee57b21131000936f1"
    response = requests.get(url)
    events = response.json()
    return events


def _get_dfs_events_list() -> list:
    """Gets the events from Data Golf DFS endpoint."""
    url = "https://feeds.datagolf.com/historical-dfs-data/event-list?&key=9e89123f99ee57b21131000936f1"
    response = requests.get(url)
    events = response.json()
    return events


def pre_tournament_predictions_archive() -> pd.DataFrame:
    """
    Collects the data from the pre-tournament predictions archive on data golf.
    """
    base_url = "https://feeds.datagolf.com/preds/pre-tournament?event_id=$$EVENT$$&year=$$YEAR$$&odds_format=percent&key=9e89123f99ee57b21131000936f1"
    df = pd.DataFrame()
    events = _get_events_list()
    for event in events:
        print("Event Name: ", event.get("event_id"))
        print("Event year: ", event.get("calendar_year"))
        if event.get("archived_preds") == "no":
            continue
        event_url = base_url.replace(
            "$$YEAR$$", str(event.get("calendar_year"))
        ).replace("$$EVENT$$", str(event.get("event_id")))
        response = requests.get(event_url)
        event_df = pd.DataFrame(response.json()["baseline"])
        event_df["event_id"] = event.get("event_id")
        event_df["year"] = event.get("calendar_year")
        event_df["event_name"] = event.get("event_name")
        if not df.empty:
            df = pd.concat([df, event_df])
        else:
            df = event_df
    df.to_csv(
        "data/interim/data_golf_pre_tournament_predictions_archive.csv",
        index=False,
    )
    return df


def historical_outrights() -> pd.DataFrame:
    """Collects the data from the Historical Outrights on data golf."""
    events = _get_events_list()
    base_url = "https://feeds.datagolf.com/historical-odds/outrights?tour=pga&event_id=$$EVENT$$&year=$$YEAR$$&market=$$MARKET$$&book=$$BOOK$$&odds_format=percent&key=9e89123f99ee57b21131000936f1"
    all_event_df = pd.DataFrame()
    for event in events:
        print("Event Name: ", event.get("event_id"))
        year = event.get("calendar_year")
        print("Event Year: ", year)
        event_url = base_url.replace("$$EVENT$$", str(event.get("event_id"))).replace(
            "$$YEAR$$", str(year)
        )

        event_df = pd.DataFrame()
        for market in MARKETS:
            for book in BOOKS:
                event_market_book_url = event_url.replace("$$MARKET$$", market).replace(
                    "$$BOOK$$", book
                )
                try:
                    response = requests.get(event_market_book_url)
                    data = response.json()
                except Exception as e:
                    print(e)
                    continue
                odds = data.get("odds")
                if not isinstance(odds, list):
                    continue
                event_market_book_df = pd.DataFrame(odds)
                event_market_book_df = event_market_book_df[
                    ["open_odds", "close_odds", "dg_id"]
                ]
                event_market_book_df.rename(
                    columns={
                        "open_odds": f"{book}_{market}_open_odds",
                        "close_odds": f"{book}_{market}_close_odds",
                    },
                    inplace=True,
                )
                if event_df.empty:
                    event_market_book_df["event_id"] = data.get("event_id")
                    event_market_book_df["event_name"] = data.get("event_name")
                    event_market_book_df["year"] = year
                    event_df = event_market_book_df.copy()
                else:
                    event_df = pd.merge(
                        left=event_df,
                        right=event_market_book_df,
                        on="dg_id",
                        how="outer",
                    )
        if not all_event_df.empty:
            all_event_df = pd.concat([all_event_df, event_df])
        else:
            all_event_df = event_df
        print("All Event DF", all_event_df.shape)
    all_event_df.to_csv("data/interim/data_golf_historical_betting.csv", index=False)
    return all_event_df


def historical_dfs_data() -> pd.DataFrame:
    """Collects the data from the Historical DFS on data golf."""
    events = (
        _get_events_list()
    )  # use event list from historical raw given left join will occur on historical raw
    base_url = "https://feeds.datagolf.com/historical-dfs-data/points?tour=pga&site=$$SITE$$&event_id=$$EVENT$$&year=$$YEAR$$&key=9e89123f99ee57b21131000936f1"
    all_event_df = pd.DataFrame()
    for event in events:
        print("Event Name: ", event.get("event_id"))
        year = event.get("calendar_year")
        print("Event Year: ", year)
        event_url = base_url.replace("$$EVENT$$", str(event.get("event_id"))).replace(
            "$$YEAR$$", str(year)
        )
        event_df = pd.DataFrame()
        for site in SITES:
            event_site_url = event_url.replace("$$SITE$$", site)
            try:
                response = requests.get(event_site_url)
                data = response.json()
            except Exception as e:
                print(e)
                continue
            dfs_output = data.get("dfs_points")
            if not isinstance(dfs_output, list):
                continue
            event_site_df = pd.DataFrame(dfs_output)
            event_site_df = event_site_df[
                [
                    "salary",
                    "ownership",
                    "hole_score_pts",
                    "finish_pts",
                    "total_pts",
                    "fin_text",
                    "dg_id",
                ]
            ]
            event_site_df.rename(
                columns={
                    "salary": f"{site}_salary",
                    "ownership": f"{site}_ownership",
                    "hole_score_points": f"{site}_hole_score_points",
                    "finish_points": f"{site}_finish_points",
                    "total_pts": f"{site}_total_pts",
                    "fin_text": f"{site}_fin_text",
                },
                inplace=True,
            )
            if event_df.empty:
                event_site_df["event_id"] = data.get("event_id")
                event_site_df["year"] = year
                event_site_df["event_name"] = data.get("event_name")
                event_df = event_site_df.copy()
            else:
                event_df = pd.merge(
                    left=event_df,
                    right=event_site_df,
                    on="dg_id",
                    how="outer",
                )
        if not all_event_df.empty:
            all_event_df = pd.concat([all_event_df, event_df])
        else:
            all_event_df = event_df
        print("All Event DF", all_event_df.shape)
    all_event_df.to_csv("data/interim/data_golf_historical_dfs.csv", index=False)
    return all_event_df


def create_data_golf_df(use_csv=False):
    """Creates the dataframe for data golf."""
    if use_csv:
        ho_df = pd.read_csv("data/interim/data_golf_historical_betting.csv")
        hdfs_df = pd.read_csv("data/interim/data_golf_historical_dfs.csv")
        ptp_df = pd.read_csv(
            "data/interim/data_golf_pre_tournament_predictions_archive.csv"
        )
    else:
        ho_df = historical_outrights()
        hdfs_df = historical_dfs_data()
        ptp_df = pre_tournament_predictions_archive()

    for df in [ho_df, hdfs_df, ptp_df]:
        df[["dg_id", "event_id", "year"]] = df[["dg_id", "event_id", "year"]].apply(
            pd.to_numeric
        )
    df_0 = pd.merge(
        left=ho_df,
        right=hdfs_df,
        on=["dg_id", "event_id", "year"],
        how="outer",
        suffixes=("", "_y"),
    )
    df_0.drop(df_0.filter(regex="_y$").columns, axis=1, inplace=True)
    final_df = pd.merge(
        left=df_0,
        right=ptp_df,
        on=["dg_id", "event_id", "year"],
        how="outer",
        suffixes=("", "_y"),
    )
    final_df.drop(final_df.filter(regex="_y$").columns, axis=1, inplace=True)
    final_df.dropna(axis=1, thresh=math.ceil(final_df.shape[0] * 0.2), inplace=True)
    final_df.to_csv("data/interim/data_golf_final.csv", index=False)


def create_course_history():
    """Creates the data golf course history csv."""
    ch_url = "https://datagolf.com/course-table?sort_cat=scoring&sort=adj_score_to_par&diff=easiest"
    try:
        res = requests.get(ch_url)
        data = res.text
    except Exception as e:
        print(e)

    # use a regex to grab the data from the HTML
    regex = r"var reload_data = JSON.parse\('(.*)'\);"
    matches = re.findall(regex, data, re.MULTILINE)
    if len(matches) != 1:
        raise Exception("Data Golf fuckery! HTML changed.")
    course_data = json.loads(matches[0])
    # we'll use this as the base by which we'll build the historical dataframe on
    course_info_df = pd.DataFrame(course_data.get("data"))[
        ["course_num", "course_name"]
    ]
    historical_course_df = pd.DataFrame()
    for course_num, by_year_list in course_data.get("by_years").items():
        print("Working on course: ", course_num)
        course_year_df = pd.DataFrame(by_year_list)
        course_year_df["course_num"] = int(course_num)
        course_year_with_info_df = pd.merge(
            left=course_year_df, right=course_info_df, on="course_num", how="left"
        )
        if historical_course_df.empty:
            historical_course_df = course_year_with_info_df.copy()
        else:
            historical_course_df = pd.concat(
                [historical_course_df, course_year_with_info_df]
            )
    historical_course_df.to_csv(
        "data/interim/data_golf_course_history.csv", index=False
    )
    return historical_course_df
