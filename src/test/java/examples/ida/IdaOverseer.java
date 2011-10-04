package examples.ida;

import examples.ida.request.IdaConnectorParams;
import gov.usgs.cida.nude.connector.IConnector;
import gov.usgs.cida.nude.connector.http.HttpConnector;
import gov.usgs.cida.nude.gel.GelBuilder;
import gov.usgs.cida.nude.gel.GelStack;
import gov.usgs.cida.nude.gel.GelledResultSet;
import gov.usgs.cida.nude.out.Dispatcher;
import gov.usgs.cida.nude.out.TableResponse;
import gov.usgs.cida.nude.overseer.Overseer;
import gov.usgs.cida.nude.params.OutputFormat;
import gov.usgs.cida.nude.provider.http.HttpProvider;
import gov.usgs.cida.nude.resultset.CGResultSet;
import gov.usgs.cida.nude.table.Column;
import gov.usgs.cida.nude.table.ColumnGrouping;
import gov.usgs.cida.nude.table.DummyColumn;
import gov.usgs.cida.spec.formatting.ReturnType;
import gov.usgs.cida.spec.out.StreamResponse;
import gov.usgs.webservices.framework.basic.FormatType;

import java.io.IOException;
import java.io.Writer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLStreamException;

public class IdaOverseer extends Overseer {
	
	protected static GelStack gelParamsIn;
	
	/**
	 * Use like this:<br>
	 * <code>connectors.get("something").getConstructor(HttpProvider.class).newInstance(httpProvider);</code>
	 */
	protected static Map<String, Class<? extends HttpConnector>> connectors;
	
	static {
		connectors = buildAvailableConnectors();
		
		gelParamsIn = buildInputGelStack();
	}
	
	protected List<CGResultSet> inputs;
	
	protected HttpProvider httpProvider;
	
	public IdaOverseer(HttpProvider httpProvider) {
		this.inputs = new ArrayList<CGResultSet>();
		this.httpProvider = httpProvider;
	}
	
	@Override
	public void addInput(CGResultSet in) {
		this.inputs.add(in);
	}

	@Override
	public void dispatch(Writer out) throws SQLException, XMLStreamException, IOException {
		// run the inputs through the GelStack
		GelledResultSet params = gelParamsIn.filter(inputs);
		
		OverseerRequest req = configureRequest(params);
		List<? extends IConnector> requestedConnectors = req.reqConnectors;
		GelStack gelOut = req.outputFilter;
		
		// Get the ResultSets from the Connectors.
		List<CGResultSet> outputs = queryConnectors(requestedConnectors);
		
		// GelStack the ResultSets
		GelledResultSet results = gelOut.filter(outputs);
		// pass the output to the dispatcher
		StreamResponse outStrm = Dispatcher.buildFormattedResponse(ReturnType.xml, FormatType.XML, new TableResponse(results));
		StreamResponse.dispatch(outStrm, out);
	}
	
	public static OverseerRequest configureRequest(
			GelledResultSet params) {
		OverseerRequest result = null;
		
		List<HttpConnector> cons = new ArrayList<HttpConnector>();
		//TODO configure
		
		GelStack gelOut = new GelStack();
		//TODO configure
		
		return result;
	}

	public static class OverseerRequest {
		public final List<? extends IConnector> reqConnectors;
		public final GelStack outputFilter;
		
		public OverseerRequest(List<? extends IConnector> reqConnectors, GelStack outputFilter) {
			this.reqConnectors = reqConnectors;
			this.outputFilter = outputFilter;
		}
	}
	
	public static List<CGResultSet> queryConnectors(List<? extends IConnector> reqConnectors) {
		List<CGResultSet> result = new ArrayList<CGResultSet>();
		
		if (null != reqConnectors) {
			for (IConnector con : reqConnectors) {
				if (con.isReady()) {
					CGResultSet resp = con.getResultSet();
					if (null != resp) {
						result.add(resp);
					}
				} else {
					throw new RuntimeException(con.getClass().getName() + " is not ready.");
				}
			}
		}
		
		return result;
	}
	
	public static Map<String, Class<? extends HttpConnector>> buildAvailableConnectors() {
		Map<String, Class<? extends HttpConnector>> result = null;
		
		result = new HashMap<String, Class<? extends HttpConnector>>();
		result.put("METADATA_REQUEST", IdaMetadataConnector.class);
		result.put("DATA_REQUEST", IdaDataConnector.class);
		
		return result;
	}
	
	public static ColumnGrouping buildExpectedUserInput() {
		ColumnGrouping result = null;
		
		List<Column> cols = new ArrayList<Column>();
		cols.add(DummyColumn.JOIN);
		cols.addAll(Arrays.asList(OutputFormat.values()));
		cols.addAll(Arrays.asList(IdaConnectorParams.values()));
		result = new ColumnGrouping(DummyColumn.JOIN, cols);
		
		return result;
	}
	
	public static GelStack buildInputGelStack() {
		GelStack result = null;
		
		result = new GelStack();
		GelBuilder gb = new GelBuilder(buildExpectedUserInput());
		result.addGel(gb.buildGel());
		
		//TODO add more transforms for configuration
		
		return result;
	}

}