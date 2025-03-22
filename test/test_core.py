from stage_model_builder.main import (
    camel_to_snake,
    get_column_names,
    get_column_aliases_sql,
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

    processed_column_names = [camel_to_snake(col) for col in test_columns]

    output = get_column_aliases_sql(test_columns)
    test_obj = zip(test_columns, processed_column_names, output)

    for raw, processed, as_statement in test_obj:
        assert f"{raw} as {processed}" == as_statement


def test_build_stage_model():
    select_statements = ["col_1", "col_2"]

    assert "select col_1, col_2 from" == get_stage_model_string(select_statements)
