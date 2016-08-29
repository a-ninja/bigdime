package io.bigdime.core.commons;

public class Segment {
	private byte[] lines;
	private byte[] leftoverData;

	public byte[] getLines() {
		return lines;
	}

	public void setLines(byte[] lines) {
		this.lines = lines;
	}

	public byte[] getLeftoverData() {
		return leftoverData;
	}

	public void setLeftoverData(byte[] leftoverData) {
		this.leftoverData = leftoverData;
	}

}
