/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.hadoop.serialization.builder;

import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.serialization.FieldType;
import org.opensearch.hadoop.serialization.Parser;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Timestamp;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class JdkValueReaderTest {
    @Test
    public void testReadValue() {
        JdkValueReader reader = new JdkValueReader();
        Parser parser = Mockito.mock(Parser.class);

        Mockito.when(parser.currentToken()).thenReturn(Parser.Token.VALUE_STRING);
        Timestamp timestamp = (Timestamp) reader.readValue(parser, "2015-01-01T12:10:30.123456789Z", FieldType.DATE_NANOS);
        assertEquals(1420114230123l, timestamp.getTime());
        assertEquals(123456789, timestamp.getNanos());

        Mockito.when(parser.currentToken()).thenReturn(Parser.Token.VALUE_NUMBER);
        Mockito.when(parser.longValue()).thenReturn(1420114230123l);
        Date date = (Date) reader.readValue(parser, "1420114230123", FieldType.DATE_NANOS);
        assertEquals(1420114230123l, date.getTime());

        Settings settings = Mockito.mock(Settings.class);
        Mockito.when(settings.getMappingDateRich()).thenReturn(false);
        reader.setSettings(settings);
        Mockito.when(parser.currentToken()).thenReturn(Parser.Token.VALUE_STRING);
        String stringValue = (String) reader.readValue(parser, "2015-01-01T12:10:30.123456789Z", FieldType.DATE_NANOS);
        assertEquals("2015-01-01T12:10:30.123456789Z", stringValue);

        Mockito.when(parser.currentToken()).thenReturn(Parser.Token.VALUE_NUMBER);
        Mockito.when(parser.longValue()).thenReturn(1420114230123l);
        Long dateLong = (Long) reader.readValue(parser, "1420114230123", FieldType.DATE_NANOS);
        assertEquals(1420114230123l, dateLong.longValue());
    }
}