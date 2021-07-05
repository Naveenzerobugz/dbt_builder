import inspect
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Text, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.sql import func
from sqlalchemy_json_querybuilder.querybuilder.search import Search
import dynamic_models

COLUMN_TYPE_MAPPING = {
    'String': String,
    'Integer': Integer
}

load_dotenv(verbose=True)

class TransformGenerator(object):

    def __init__(self):
        self.base = declarative_base()
        con_url = 'postgresql://{username}:{password}@{host}:{port}/{database}'.format(
    username=os.getenv('username'), password=os.getenv('password'), host=os.getenv('host'), port=os.getenv('port'), database=os.getenv('database')
)  # TODO not sure what this url does. using a dummy url to
        # create the session and query
        self.engine = create_engine(con_url, pool_recycle=3600)
        self.session_maker = sessionmaker(bind=self.engine, autoflush=True, autocommit=False, expire_on_commit=True)
        self.session = scoped_session(self.session_maker)
        self.model_classes = {}

    def generate(self, flow_data):
        for model_data in flow_data['models']:
            stmt = self.generate_for_model(model_data)
            print(stmt)

    def generate_for_model(self, model_data):
        prev_model_class = self.build_model_class(model_data['output_cols'])
        model_type=model_data['model_type']
        print(model_type)
        if  model_type !=  "source":
            builder = AbstractQueryBuilder.get_builder(model_data, prev_model_class)
            stmt = builder.generate_query(self.session)
            return stmt

    def build_model_class(self, prev_model_data):

        model_name = 'model_{}'.format(10)
        model_class = self.model_classes.get(model_name)
        if model_class is None:
            output_cols = prev_model_data
            model_attrs = {'__tablename__': model_name}
            for col in output_cols:
                model_attrs[col['name']] = Column(COLUMN_TYPE_MAPPING.get(col['col_type']))
            model_attrs['meta_primary_key'] = Column(COLUMN_TYPE_MAPPING.get('String'), primary_key=True)
            model_class = type(model_name, (self.base,), model_attrs)
            setattr(dynamic_models, model_class.__name__, model_class)
            self.model_classes[model_name] = model_class

        return model_class


class AbstractQueryBuilder(object):

    def __init__(self, model_data, prev_model_class):
        self.model_data = model_data
        self.model_class = prev_model_class

    @classmethod
    def get_builder(cls, model_data, prev_model_class):
        model_type = model_data['model_type']
        if model_type == 'filter':
            return FilterQueryBuilder(model_data, prev_model_class)
        elif model_type == 'aggregate':
            return AggregateQueryBuilder(model_data, prev_model_class)
        elif model_type == 'source':
            print("source")
        else:
            raise NotImplementedError('model type not supported!')

    def generate_query(self, session):
        raise NotImplementedError


class FilterQueryBuilder(AbstractQueryBuilder):

    def generate_query(self, session):
        groupcount=self.model_data['model_config']['criteria']['groupcount']
        criteria=self.model_data['model_config']['criteria']
        filter_by={}
        test={}
        for i in range(groupcount): 
            # filter_by[f"or"]=  criteria['rules'][f"group_{i+1}"]
            filter_by=  {"or":criteria['rules'][f"group_{i+1}"]}
            print(filter_by)
            test={
                *test,
                filter_by
            }
  
        print(filter_by)
        # criteria = self.model_data['model_config']['criteria']
        # output_cols = self.model_data['prev_model']['output_cols']
        # search = Search(session, 'transforms.dynamic_models', (self.model_class,), filter_by=criteria)
        # query = search.query()
        # return query.statement.compile(compile_kwargs={"literal_binds": True})


class AggregateQueryBuilder(AbstractQueryBuilder):

    def generate_query(self, session):
        agg_type = self.model_data['model_config']['agg_type']
        model_class = self.model_class
        model_config = self.model_data['model_config']
        agg_col = model_class.__table__.c[model_config['agg_cols'][0]['value']]
        group_by_col = model_class.__table__.c[model_config['group_by_cols'][0]['value']]
        if agg_type == 'sum':
            agg_func = func.sum(agg_col)
        elif agg_type == 'max':
            agg_func = func.max(agg_col)
        elif agg_type == 'min':
            agg_func = func.min(agg_col)
        elif agg_type == 'avg':
            agg_func = func.avg(agg_col)
        else:
            raise NotImplementedError('function not supported!')
        if 'label' in model_config:
            agg_func = agg_func.label(model_config['agg_cols'][0]['label'])
        query = session.query(agg_func)
        query = query.select_from(model_class).group_by(group_by_col)
        return query.statement.compile(compile_kwargs={"literal_binds": True})
