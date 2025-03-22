from stage_model_builder.main import (
    camel_to_snake,
    get_column_names,
    generate_sql_column_aliases,
    get_stage_model_string,
)


def test_camel_to_snake():
    assert camel_to_snake("camelCaseObject") == "camel_case_object"
    assert camel_to_snake("sname_case_object") == "sname_case_object"
    assert camel_to_snake("CapitalCaseObject") == "capital_case_object"
    assert camel_to_snake("ALLUPPERCASE") == "alluppercase"
    assert camel_to_snake("alllowercase") == "alllowercase"


def test_parquet_schema_extract():
    columns = get_column_names("test/test_object.parquet")
    assert columns == ["name", "id", "res"]


def test_get_column_aliases():
    test_columns = [
        "test_column_one",
        "TestCol2",
    ]

    expected_output = [
        "test_column_one",
        "TestCol2 as test_col2",
    ]

    assert expected_output == generate_sql_column_aliases(test_columns)


def test_build_stage_model():
    select_statements = ["col_1", "col_2"]

    assert "select col_1, col_2 from" == get_stage_model_string(select_statements)
