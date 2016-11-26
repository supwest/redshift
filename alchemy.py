import sys,os,csv,re
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine
from sqlalchemy.orm import Session,relationship
from sqlalchemy import MetaData,Table,Column,Sequence,ForeignKey,Integer, String
from sqlalchemy.inspection import inspect
from sqlalchemy.sql import select
from sqlalchemy.ext.declarative import declarative_base


try:
    from sqlalchemy_schemadisplay import create_schema_graph
    createGraph = True
except:
    createGraph= False

class DbWrapper(object):
    """
    Interface with db
    """
    def __init__(self, uname, upass, dbname, dbhost='localhost', port='5439', reflect=False):
        """
        Constructor

        uname - database username
        upass - database password
        dbname - database name
        dbhost - database host
        port - database port
        """

        self.uname = uname
        self.upass = upass
        self.dbname = dbname
        self.dbhost = dbhost
        self.port = port

        self.connect()

    def connect(self):
        self.Base = automap_base()
        self.engine = create_engine("redshift+psycopg2://{}:{}@{}:{}/{}".format(self.uname, self.upass, self.dbhost, self.port, self.dbname))
        self.conn = self.engine.connect()
        self.session = Session(self.engine)
        self.meta = MetaData()
        self.tables = {}

        #reflect the tables
        self.meta.reflect(bind=self.engine)
        for tname in self.engine.table_names():
            tbl = Table(tname, self.meta, autoload=True, autoload_with=self.engine)
            self.tables[tname] = tbl

    def print_summary(self):
        """
        print a list of the tables
        """

        print("-"*15)
        print("{}".format(self.dbname))
        print("{} tables".format(len(self.tables.keys())))

        for tname, tbl in self.tables.iteritems():
            print("\t {}".format(tname))
            print("\t\tPK: {} ".format(";".join([key.name for key in inspect(tbl).primary_key])))
            for col in tbl.columns:
                print("\t\t{}".format(col))
        
    def draw_schema(self, filename='schema.png'):
        if createGraph:
            graph = create_schema_graph(metadata=self.meta,
                                        show_datatypes=False,
                                        show_indexes=False,
                                        rankdir='LRA',
                                        concentrate=False)
            if re.search("\.png",filename):
                #graph.write_png(filename)
                graph.write_png('schema.png')
            elif re.search("\.svg",filename):
                graph.write_svg(filename)
            else:
                raise Excpetion("invalid filename specified")

            print("... {} created".format(filename))

        else:
            print("not creating fig because thing not installed")

#//examplecluster.cngdbfmbk6nh.us-west-2.redshift.amazonaws.com:5439.


if __name__ == '__main__':
    passwd = os.environ['EX_REDSHIFT_PASS']
    host = 'examplecluster.cngdbfmbk6nh.us-west-2.redshift.amazonaws.com'
    db = DbWrapper('masteruser', passwd, 'dev', host, 5439)
    db.print_summary()
    #db = DbWrapper('masteruser', passwd, db_name, db_host, port) 
    db.draw_schema()


    Venue = db.tables['venue']
    all_venues = db.session.query(Venue).all()
    las_vegas_venues = db.session.query(Venue).filter_by(venuecity='Las Vegas').all()
    denver_kc_venues = db.session.query(Venue).filter(Venue.c.venuecity.in_(['Denver', 'Kansas City'])).all()
    Event = db.tables['event']
    #Arrowhead id is 79
    kc_events = db.session.query(Event, Venue).filter(Event.c.venueid ==79).filter(Event.c.venueid==Venue.c.venueid).all()
    
    s = select([Venue])
    result = db.conn.execute(s).fetchall()
    print str(s)
    print result[0]
