package examples.ida;

import examples.ida.response.IdaMetadata;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.connector.http.AbstractHttpParser;
import java.io.BufferedReader;

import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IdaParser extends AbstractHttpParser {
	
	protected IdaMetadata[] columns = IdaMetadata.values();
		
	protected EnumMap<IdaMetadata, String> row = new EnumMap<IdaMetadata, String>(IdaMetadata.class);
	
	protected static final Pattern pat = Pattern.compile("^\\s*<input.* name=\"mindatetime\".* value=\"([^\"]*)\".*\\/>.*<input.* name=\"maxdatetime\".* value=\"([^\"]*)\".*\\/>.*<input.*");

	public IdaParser() {
		this(new ColumnGrouping((Column) IdaMetadata.MINDATETIME, Arrays.asList((Column[]) IdaMetadata.values())));
	}
	
	public IdaParser(ColumnGrouping cg) {
		super(cg);
	}
	
	@Override
	public boolean next(Reader in) throws SQLException {
		this.row.clear();
		boolean result = false;
		
		BufferedReader inBuf = (BufferedReader) in;
		
		try {
			boolean endOfStream = false;
			while (!result && !endOfStream) {
				String line = inBuf.readLine();
				if (null == line) {
					endOfStream = true;
				} else {
					Matcher mat = pat.matcher(line);
					if (mat.matches() && 2 == mat.groupCount()) {
						row.put(IdaMetadata.MINDATETIME, mat.group(1));
						row.put(IdaMetadata.MAXDATETIME, mat.group(2));
						result = true;
					}
				}
			}
		} catch (IOException e) {
			throw new SQLException(e);
		}
		
		return result;
	}

	@Override
	public String getValue(int columnIndex) throws SQLException {
		String result = null;
		if (-1 < columnIndex && columnIndex < this.columns.length) {
			result = this.row.get(this.columns[columnIndex]);
		} else {
			throw new SQLException("Invalid Column");
		}
		return result;
	}

}
