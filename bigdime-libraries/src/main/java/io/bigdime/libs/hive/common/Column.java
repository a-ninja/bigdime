/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hive.common;

public final class Column {
	private static final String STRING_TYPE = "STRING";
	public Column()
	{
		
	}
	
	public Column(String name, String type, String comment) {
		this.name = name;
		this.type = type;
		this.comment = comment;
	}
	
	private String name;
	private String type;
	private String comment;
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		if(type == null){
			type = STRING_TYPE;	
		}
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getComment() {
		return comment;
	}
	
	public void setComment(String comment) {
		this.comment = comment;
	}	
	@Override
	public String toString(){
		return name + ":" + type;
		
	}
}
