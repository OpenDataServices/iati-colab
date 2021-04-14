# -*- coding: utf-8 -*-

import pandas as pd
import requests as rq
import lxml.etree as ET
import json
import copy
import subprocess
import pathlib
from collections import defaultdict

from bdd_tester import BDDTester


def remove_comments(etree):
    comments = etree.xpath("//comment()")
    for c in comments:
        p = c.getparent()
        p.remove(c)
    return etree


def get_datasets(registry_id="", exceptions="", include_urls=""):
    datasets = []
    if registry_id:
        registries = registry_id.split()
        for registry in registries:
            exceptions = exceptions or []
            dataset_df = pd.read_csv("https://iatiregistry.org/csv/download/" + registry)
            dataset_df = dataset_df[dataset_df["file-type"] != "organisation"]
            dataset_df = dataset_df[~dataset_df["registry-file-id"].isin(exceptions)]
            datasets.extend(dataset_df['source-url'].tolist())

    datasets.extend(include_urls.split())

    return datasets


def all_activities(datasets):
    print("Removed unwanted activities and setup comment-removal method")

    print("\nCombining {} IATI files \n".format(len(datasets)))

    # Start with the first file, with comments removed
    big_iati = remove_comments(ET.fromstring(rq.get(datasets[0]).content))

    # Start a dictionary to keep track of the additions
    merge_log = {datasets[0]: len(big_iati.getchildren())}

    # Iterate through the 2nd through last file and
    # insert their activtities to into the first
    # and update the dictionary
    for url in datasets[1:]:
        data = remove_comments(ET.fromstring(rq.get(url).content))
        merge_log[url] = len(data.getchildren())
        big_iati.extend(data.getchildren())

    # Print a small report on the merging
    print("Files Merged: ")
    for file, activity_count in merge_log.items():
        print("|-> {} activities from {}".format(activity_count, file))

    print("|--> {} in total".format(len(big_iati.getchildren())))

    return big_iati


def current_activities(all_activities):

    import datetime as dt
    from dateutil.relativedelta import relativedelta

    # Filter out non-current activities, if appropriate
    # See https://github.com/pwyf/latest-index-indicator-definitions/issues/1

    log_columns = [
        "iati-id",
        "status_check",
        "planned_end_date_check",
        "actual_end_date_check",
        "transaction_date_check",
        "pwyf_current",
    ]
    count = 1
    current_check_log = pd.DataFrame(columns=log_columns)

    for activity in all_activities:

        status_check = False
        planned_end_date_check = False
        actual_end_date_check = False
        transaction_date_check = False

        # print("Activity {} of {}".format(count, len(big_iati)))

        if activity.xpath("activity-status[@code=2]"):
            status_check = True

        if activity.xpath("activity-date[@type=3]/@iso-date"):
            date_time_obj = dt.datetime.strptime(activity.xpath("activity-date[@type=3]/@iso-date")[0], "%Y-%m-%d")
            if date_time_obj > (dt.datetime.now() - relativedelta(years=1)):
                planned_end_date_check = True

        if activity.xpath("activity-date[@type=4]/@iso-date"):
            date_time_obj = dt.datetime.strptime(activity.xpath("activity-date[@type=4]/@iso-date")[0], "%Y-%m-%d")
            if date_time_obj > (dt.datetime.now() - relativedelta(years=1)):
                actual_end_date_check = True

        if activity.xpath("transaction/transaction-type[@code=2 or @code=3 or @code=4]"):
            dates = activity.xpath(
                "transaction[transaction-type[@code=2 or @code=3 or @code=4]]/transaction-date/@iso-date"
            )
            date_truths = [
                dt.datetime.strptime(date, "%Y-%m-%d") > (dt.datetime.now() - relativedelta(years=1)) for date in dates
            ]
            if True in date_truths:
                transaction_date_check = True

        pwyf_current = status_check or planned_end_date_check or actual_end_date_check or transaction_date_check

        current_check_log = current_check_log.append(
            {
                "iati-id": activity.findtext("iati-identifier"),
                "status_check": status_check,
                "planned_end_date_check": planned_end_date_check,
                "actual_end_date_check": actual_end_date_check,
                "transaction_date_check": transaction_date_check,
                "pwyf_current": pwyf_current,
            },
            ignore_index=True,
        )

        count = count + 1

    current_check_log.to_csv("current_check_log.csv")

    current_activities = copy.deepcopy(all_activities)

    cur_length = len(current_activities)

    for activity in current_activities:
        if (
            activity.findtext("iati-identifier")
            in current_check_log.loc[current_check_log["pwyf_current"] == False, "iati-id"].values
        ):
            activity.getparent().remove(activity)

    print("Removed {} non-current activities from a total of {}.".format((cur_length - len(current_activities)), cur_length))
    print("{} current activities remain.".format(len(current_activities)))

    return current_activities


def coverage_check(tree, path, manual_list_entry=False):
    if manual_list_entry:
        denominator = len(tree)
        numerator = len(path)
    else:
        denominator = len(tree.getchildren())
        numerator = len(tree.xpath(path))

    coverage = numerator / denominator
    return denominator, numerator, coverage


def cove_validation(activities):

    with open("combined.xml", "wb+") as out_file:
        out_file.write(ET.tostring(activities, encoding="utf8", pretty_print=True))

    json_validation_filepath = "validation.json"
    url = "https://iati.cove.opendataservices.coop/api_test"
    files = {"file": open("combined.xml", "rb")}
    r = rq.post(url, files=files, data={"name": "combined.xml"})

    print(r)

    print("CoVE validation was successful." if r.ok else "Something went wrong.")

    validation_json = r.json()

    with open(json_validation_filepath, "w") as out_file:
        json.dump(validation_json, out_file)

    print("Validation JSON file has been written to {}.".format(json_validation_filepath))

    ruleset_table = pd.DataFrame(data=validation_json["ruleset_errors"])
    schema_table = pd.DataFrame(data=validation_json["validation_errors"])
    embedded_codelist_table = pd.DataFrame(data=validation_json["invalid_embedded_codelist_values"])
    non_embedded_codelist_table = pd.DataFrame(data=validation_json["invalid_non_embedded_codelist_values"])

    print(
        "CoVE has found: \n* {} schema errors \n* {} ruleset errors \n* {} embedded codelist errors \n* {} non-embedded codelist errors".format(
            len(schema_table), len(ruleset_table), len(embedded_codelist_table), len(non_embedded_codelist_table)
        )
    )

    print("\nWriting to validation_workbook.xlsx")
    writer = pd.ExcelWriter("validation_workbook.xlsx", engine="xlsxwriter")
    # Write each dataframe to a different worksheet.
    schema_table.to_excel(writer, sheet_name="schema_table")
    ruleset_table.to_excel(writer, sheet_name="ruleset_table")
    embedded_codelist_table.to_excel(writer, sheet_name="embedded_codelist_table")
    non_embedded_codelist_table.to_excel(writer, sheet_name="non_embedded_codelist_table")

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()

    """### Schema Validation"""

    return schema_table, ruleset_table, embedded_codelist_table, non_embedded_codelist_table


content_path = None
test_definitions_path = None
steps_path = None
tester = None


def setup_indicator_definitions(repo='https://github.com/pwyf/latest-index-indicator-definitions.git', branch=None):
    cmd = f"""
        git clone {repo} indicator_definitions
    """
    if branch:
        cmd += f'-b {branch}'

    process = subprocess.run(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    print(process.stderr.decode())

    global content_path
    global test_definitions_path
    global steps_path
    global tester

    content_path = pathlib.Path('/content')
    test_definitions_path = content_path / 'indicator_definitions' / 'test_definitions'
    steps_path = test_definitions_path / 'step_definitions.py'
    tester = BDDTester(str(steps_path))


def show_tests():
    for feature_path in sorted(test_definitions_path.glob('**/*.feature'), key=lambda a: a.name):
        feature = tester.load_feature(str(feature_path))
        print(feature_path.name)
        for num, test in enumerate(feature.tests):
            print(f'    {num+1}. {test.name}')


def get_test(name, test_number=1):
    for feature_path in test_definitions_path.glob('**/*.feature'):
        if feature_path.name.startswith(str(name)):
            found_path = str(feature_path)
            break
    else:
        raise Exception('Feature not found')

    feature = tester.load_feature(str(found_path))

    return feature.tests[test_number - 1]


def test_activities(activities, name, test_number=1, **kw):
    results = defaultdict(list)
    test = get_test(name, test_number)
    print(f'running test {test.name}')
    for num, activity in enumerate(activities.findall('iati-activity')):
        result = test(activity, **kw)
        results[result].append(activity)
    for key, value in results.items():
        print(f'{key} -> {len(value)}')
    return results


def test_activity(activity, name, test_number=1, **kw):
    test = get_test(name, test_number)
    print(f'running test: {test.name}')
    activity = ET.fromstring(activity)
    return test(activity, **kw)
