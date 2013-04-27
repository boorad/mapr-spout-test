package com.mapr.demo.twitter;

import java.io.File;
import java.io.IOException;


/*******************************************************************************
 * Copyright (c) 2007 Pascal Essiembre. All rights reserved. This program and
 * the accompanying materials are made available under the terms of the Eclipse
 * Public License v1.0 which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors: Pascal Essiembre - initial API and implementation
 ******************************************************************************/

/**
 * Listener interested in {@link File} changes.
 *
 * @author Pascal Essiembre
 */
interface FileChangeListener {
  /**
   * Invoked when a file changes.
   *
   * @param fileName
   *          name of changed file.
   * @throws IOException
   */
  public void fileChanged(File file) throws IOException;
}
