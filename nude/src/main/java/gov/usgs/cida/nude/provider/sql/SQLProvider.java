package gov.usgs.cida.nude.provider.sql;

import gov.usgs.cida.nude.out.Closers;
import gov.usgs.cida.nude.provider.IProvider;
import gov.usgs.cida.nude.resultset.inmemory.TypedValue;
import java.lang.ref.WeakReference;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dmsibley
 */
public class SQLProvider implements IProvider {
	private static final Logger log = LoggerFactory.getLogger(SQLProvider.class);
	protected String jndiName = null;
	protected static Map<UUID, WeakReference<Connection>> requestToConnectionMap = new WeakHashMap<UUID, WeakReference<Connection>>();
	protected static Map<Integer, WeakReference<UUID>> hashToRequestMap = new WeakHashMap<Integer, WeakReference<UUID>>();

	public SQLProvider(String jndiName) {
		if (null == jndiName) {
			log.error("No JNDI name specified!");
		} else {
			log.trace("JNDI Name: " + jndiName);
			this.jndiName = jndiName;
		}
	}
	
	@Override
	public void init() {
		log.trace("Initializing SQLProvider " + this.hashCode());
		
		log.trace("Initialized SQLProvider " + this.hashCode());
	}

	public Connection getHoldableConnection(UUID requestId) throws SQLException, NamingException, ClassNotFoundException {
		Connection result = getConnection(requestId);

		try {
			result.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
		} catch (SQLFeatureNotSupportedException e) {
			log.warn("Unable to get holdable connection. Exception: ", e);
		}

		return result;
	}
	
	public Connection getConnection(UUID requestId) throws SQLException, NamingException, ClassNotFoundException {
		Connection result = null;
		
		Connection con = getConnection(requestId, this.jndiName);
		result = con;
		
		return result;
	}
	
	/**
	 * Gets a ResultSet for the query.  If it succeeds, you are responsible
	 * for getting the Statement and Connection through the ResultSet and closing
	 * them when you are done.
	 * @param query
	 * @return 
	 */
	public ResultSet getResults(UUID requestId, ParameterizedString query) {
		ResultSet result = null;
		
		if (null != query) {
			Connection con = null;
			try {
				con = this.getConnection(requestId);
				result = getQueryResults(query, con);
			} catch (Exception e) {
				log.error("Cannot get requests for query: " + query.toEvaluatedString(), e);
				Closers.closeQuietly(result);
				closeConnection(con);
			}
		}
		
		return result;
	}
	
	/**
	 * Gets a ResultSet for the query.  If it succeeds, you are responsible
	 * for getting the Statement and Connection through the ResultSet and closing
	 * them when you are done.
	 * @param query
	 * @return 
	 */
	public ResultSet getResults(UUID requestId, String query) {
		ResultSet result = null;
		
		if (null != query) {
			Connection con = null;
			try {
				con = this.getConnection(requestId);
				result = getQueryResults(query, con);
			} catch (Exception e) {
				log.error("Cannot get requests for query: " + query, e);
				Closers.closeQuietly(result);
				closeConnection(con);
			}
		}
		
		return result;
	}
	
	@Override
	public void destroy() {
		log.trace("Destroying SQLProvider " + this.hashCode());
		
		log.trace("Destroyed SQLProvider " + this.hashCode());
	}
	
	
	private static AtomicInteger connectionCount = new AtomicInteger(0);
	/**
	 * This variable tells us how many cycles (getConnection called -> closeConnection called)
	 * are still in process.  This will show us if there's a 1:1 ratio of getConnection vs closeConnection
	 */
	private static AtomicInteger unfinishedConCycles = new AtomicInteger(0);
	
	/**
	 * Factory method to create a Connection object to the Database
	 * @param dataSource
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws NamingException
	 */
	public static Connection getConnection(UUID requestId, String dataSource) throws SQLException, NamingException, ClassNotFoundException {
		int connectionNumber = -1;
		Connection connection = null;
		try {
			if (null != dataSource) {
				connection = getJNDIConnection(dataSource);
			}
			if (null == connection) {
				log.trace("Could not find JNDI hook. Trying simple JDBC connection.");
				connection = getJDBCConnection();
			}
			if (null != connection) {
				if (null != requestId) {
					requestToConnectionMap.put(requestId, new WeakReference<Connection>(connection));
					hashToRequestMap.put(new Integer(connection.hashCode()), new WeakReference<UUID>(requestId));
				} else {
					log.debug("no requestId specified");
				}
			}
		} finally {
			if (null != connection) {
				connectionNumber = connection.hashCode();
			}
			int conCycles = unfinishedConCycles.incrementAndGet();
			log.debug("Connection Cycle started:" + connectionNumber + ", Total Unfinished:" + conCycles);
		}
		return connection;
	}

	/**
	 * Attempts to attain a java.sql.Connection object via JDBC 
	 * 
	 * @return Open Connection object.  Null if not possible.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	private static Connection getJDBCConnection() throws SQLException, ClassNotFoundException {
		Connection connection = null;
		String dbuser 	= System.getProperty("dbuser");
		String dbpass 	= System.getProperty("dbpass");
		String url 		= System.getProperty("dburl");
		String clazz    = System.getProperty("dbclass");
        if (clazz == null) {
            clazz = "oracle.jdbc.OracleDriver";
        }

		Class.forName(clazz);
		connection = DriverManager.getConnection(url, dbuser, dbpass);
		int conCount = connectionCount.incrementAndGet();
		log.trace("Opened Testing Connection " + connection.hashCode()+ ". Total:" + conCount);
		return connection;

	}

	/**
	 * Attempts to attain a java.sql.Connection object via JNDI Naming 
	 * 
	 * @param dataSource JNDI string to connect to
	 * @return
	 */
	private static Connection getJNDIConnection(String dataSource) {
		Connection connection = null;

		Context ctx;
		try {
			ctx = new InitialContext();
			String dataSourceString = dataSource;
			DataSource ds = (DataSource) ctx.lookup(dataSourceString);
			connection = ds.getConnection();
			int conCount = connectionCount.incrementAndGet();
			log.trace("Opened Connection " + connection.hashCode()+ ". Total:" + conCount);
		} catch (NamingException e) {
			log.error("Could not open Connection object: " + e.getMessage());
			return null;
		} catch (SQLException e) {
			log.error("Could not open Connection object: " + e.getMessage());
			return null;
		}		
		return connection;
	}
	
	public static void closeConnection(UUID requestId) {
		WeakReference<Connection> con = requestToConnectionMap.get(requestId);
		if (null != con) {
			closeConnection(con.get());
		} else {
			log.trace("Cannot close, no connection for " + requestId);
		}
	}
	
	/**
	 * Closes a java.sql.Connection object 
	 * @param con java.sql.Connection that needs closing
	 */
	public static void closeConnection(Connection con) {
		int connectionNumber = -1;
		try {
			if (null != con) {
				connectionNumber = con.hashCode();
				if (con.isClosed()) {
					log.error("Connection " + connectionNumber + " is already closed!!");
				}
				log.trace("Closing Connection " + connectionNumber);
				con.close(); 
				int conCount = connectionCount.decrementAndGet();
				log.trace("Closed Connection. Total:" + conCount);
				
				WeakReference<UUID> reqId = hashToRequestMap.remove(new Integer(connectionNumber));
				if (null != reqId) {
					requestToConnectionMap.remove(reqId.get());
				} else {
					log.debug("We don't have a UUID for this? " + connectionNumber);
				}
				
			} else {
				log.trace("closeConnection called on null object");
			}
		} catch (Exception e) {
			log.error("Could not close Connection object: " + e.getMessage());
		} finally {
			int conCycles = unfinishedConCycles.decrementAndGet();
			log.debug("Connection Cycle finished:" + connectionNumber + ", Total Unfinished:" + conCycles);
		}
	}

	/**
	 * Executes a database query, the criteria for which is provided by the query parameter
	 * 
	 * @param query The criteria for the query  
	 * @param con An open database connection object
	 * @return ResultSet object containing the results of the query 
	 * @throws SQLException
	 */
	public static ResultSet getQueryResults(String query, Connection con) throws SQLException {
		Statement statement = con.createStatement();
		ResultSet rset = statement.executeQuery(query);
		return rset;
	}
	
	/**
	 * Executes a database query via a PreparedStatement
	 * 
	 * @param query The ParameterizedString criteria for the query  
	 * @param con An open database connection object
	 * @return ResultSet object containing the results of the query 
	 * @throws SQLException
	 */
	public static ResultSet getQueryResults(ParameterizedString query, Connection con) throws SQLException {
		PreparedStatement statement = prepareStatement(query, con);
		return statement.executeQuery();
	}
	
	/**
	 * Executes a database update (INSERT, UPDATE, DELETE)
	 * on a string.
	 * 
	 * @param sql The String criteria for the update  
	 * @param con An open database connection object
	 * @return int the number of rows affected by the update 
	 * @throws SQLException
	 */
	public static int doUpdate(String sql, Connection con) throws SQLException {
		Statement statement = con.createStatement();
		int result = statement.executeUpdate(sql);
		return result;
	}
	
	/**
	 * Executes a database update (INSERT, UPDATE, DELETE)
	 * via a PreparedStatement
	 * 
	 * @param sql The ParameterizedString criteria for the query  
	 * @param con An open database connection object
	 * @return int the number of rows affected by the update 
	 * @throws SQLException
	 */
	public static int doUpdate(ParameterizedString sql, Connection con) throws SQLException {
		PreparedStatement statement = prepareStatement(sql, con);
		return statement.executeUpdate();
	}

	public static void setAutoCommit(Connection con, boolean b) {
		if (null != con) {
			try {
				log.trace("CONNECTION " + con.hashCode() +" AUTO_COMMIT: " + con.getAutoCommit() + " NEW: " + b);
				if (b != con.getAutoCommit()) {
					con.setAutoCommit(b);
				}
			} catch (SQLException e) {
				log.error("Caught Exception", e);
			}
		}
	}
	
	public static PreparedStatement prepareStatement(ParameterizedString sql, Connection con) throws SQLException {
		PreparedStatement result = con.prepareStatement(sql.getClause());
		
		List<TypedValue> values = sql.getValues();
		for (int i = 0; i < values.size(); i++) {
			TypedValue val = values.get(i);
			if (null == val) {
				val = new TypedValue(null);
			}
			result.setString(i+1, val.toString());
		}
		
		return result;
	}
}
