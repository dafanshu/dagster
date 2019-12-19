import pandas as pd
from dagster_pandas.constraints import ConstraintViolationException, Constraint, ColumnTypeConstraint, \
    ColumnExistsConstraint
from dagster_pandas.validation import validate_collection_schema, PandasColumn

from dagster import (
    DagsterInvariantViolationError,
    EventMetadataEntry,
    Field,
    Materialization,
    Path,
    RuntimeType,
    String,
    TypeCheck,
    as_dagster_type,
    check,
)
from dagster.core.types.config.field_utils import NamedSelector
from dagster.core.types.runtime.config_schema import input_selector_schema, output_selector_schema


CONSTRAINT_BLACKLIST = {
    ColumnExistsConstraint,
    ColumnTypeConstraint
}

NEW_LINE = '\n\n'

def dict_without_keys(ddict, *keys):
    return {key: value for key, value in ddict.items() if key not in set(keys)}


@output_selector_schema(
    NamedSelector(
        'DataFrameOutputSchema',
        {
            'csv': {'path': Path, 'sep': Field(String, is_optional=True, default_value=','),},
            'parquet': {'path': Path},
            'table': {'path': Path},
        },
    )
)
def dataframe_output_schema(_context, file_type, file_options, pandas_df):
    check.str_param(file_type, 'file_type')
    check.dict_param(file_options, 'file_options')
    check.inst_param(pandas_df, 'pandas_df', DataFrame)

    if file_type == 'csv':
        path = file_options['path']
        pandas_df.to_csv(path, index=False, **dict_without_keys(file_options, 'path'))
    elif file_type == 'parquet':
        pandas_df.to_parquet(file_options['path'])
    elif file_type == 'table':
        pandas_df.to_csv(file_options['path'], sep='\t', index=False)
    else:
        check.failed('Unsupported file_type {file_type}'.format(file_type=file_type))

    return Materialization.file(file_options['path'])


@input_selector_schema(
    NamedSelector(
        'DataFrameInputSchema',
        {
            'csv': {'path': Path, 'sep': Field(String, is_optional=True, default_value=','),},
            'parquet': {'path': Path},
            'table': {'path': Path},
        },
    )
)
def dataframe_input_schema(_context, file_type, file_options):
    check.str_param(file_type, 'file_type')
    check.dict_param(file_options, 'file_options')

    if file_type == 'csv':
        path = file_options['path']
        return pd.read_csv(path, **dict_without_keys(file_options, 'path'))
    elif file_type == 'parquet':
        return pd.read_parquet(file_options['path'])
    elif file_type == 'table':
        return pd.read_csv(file_options['path'], sep='\t')
    else:
        raise DagsterInvariantViolationError(
            'Unsupported file_type {file_type}'.format(file_type=file_type)
        )


def df_type_check(value):
    if not isinstance(value, pd.DataFrame):
        return TypeCheck(success=False)
    return TypeCheck(
        success=True,
        metadata_entries=[
            EventMetadataEntry.text(str(len(value)), 'row_count', 'Number of rows in DataFrame'),
            # string cast columns since they may be things like datetime
            EventMetadataEntry.json({'columns': list(map(str, value.columns))}, 'metadata'),
        ],
    )


DataFrame = as_dagster_type(
    pd.DataFrame,
    name='PandasDataFrame',
    description='''Two-dimensional size-mutable, potentially heterogeneous
    tabular data structure with labeled axes (rows and columns).
    See http://pandas.pydata.org/''',
    input_hydration_config=dataframe_input_schema,
    output_materialization_config=dataframe_output_schema,
    type_check=df_type_check,
)


def construct_constraint_list(constraints):
    def add_bullet(constraint_list, constraint_name, constraint_description):
        return constraint_list+"\n"+"+ {constraint_description}".format(constraint_name=constraint_name, constraint_description=constraint_description)

    constraint_list = ""
    for constraint in constraints:
        if constraint.__class__ not in CONSTRAINT_BLACKLIST:
            constraint_list = add_bullet(constraint_list, constraint.name, constraint.description)
    return constraint_list


def _get_expected_column_types(constraints):
    column_type_constraint = [
        constraint for constraint in constraints
        if isinstance(constraint, ColumnTypeConstraint)
    ]
    if column_type_constraint:
        # TODO: You could have more than one type constraint technically but let's ignore that.
        expected_types = column_type_constraint[0].expected_pandas_dtypes
        if expected_types:
            return list(expected_types)[0] if len(expected_types) == 1 else expected_types
    return 'Any'


def create_dagster_pandas_dataframe_description(description, columns):
    description = check.opt_str_param(description, 'description', default='')
    columns = check.opt_list_param(columns, 'columns', of_type=PandasColumn)

    buildme = description + NEW_LINE + '## DataFrame Schema' + NEW_LINE


    for column in columns:
        expected_column_types = _get_expected_column_types(column.constraints)

        column_header_string = "### *{column_name}*: `{expected_types}`\n\n".format(column_name=column.name, expected_types=expected_column_types)
        buildme = buildme + column_header_string + construct_constraint_list(column.constraints) + "\n\n"
    return buildme


def create_dagster_pandas_dataframe_type(
    name=None, description=None, columns=None, summary_statistics=None
):
    summary_statistics = check.opt_callable_param(summary_statistics, 'summary_statistics')

    def _dagster_type_check(value):
        if not isinstance(value, DataFrame):
            return TypeCheck(
                success=False,
                description='Must be a pandas.DataFrame. Got value of type. {type_name}'.format(
                    type_name=type(value).__name__
                ),
            )

        if columns is not None:
            try:
                validate_collection_schema(columns, value)
            except ConstraintViolationException as e:
                return TypeCheck(success=False, description=str(e))

        return TypeCheck(
            success=True,
            metadata_entries=_execute_summary_stats(name, value, summary_statistics)
            if summary_statistics
            else None,
        )

    # add input_hydration_confign and output_materialization_config
    # https://github.com/dagster-io/dagster/issues/2027
    return RuntimeType(name=name, key=name, type_check_fn=_dagster_type_check, description=description)


def _execute_summary_stats(type_name, value, summary_statistics_fn):
    metadata_entries = summary_statistics_fn(value)

    if not (
        isinstance(metadata_entries, list)
        and all(isinstance(item, EventMetadataEntry) for item in metadata_entries)
    ):
        raise DagsterInvariantViolationError(
            (
                'The return value of the user-defined summary_statistics function '
                'for pandas data frame type {type_name} returned {value}. '
                'This function must return List[EventMetadataEntry]'
            ).format(type_name=type_name, value=repr(metadata_entries))
        )

    return metadata_entries
