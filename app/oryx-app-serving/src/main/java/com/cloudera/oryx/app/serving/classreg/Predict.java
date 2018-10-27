/*
 * Copyright (c) 2014, Cloudera and Intel, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.app.serving.classreg;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.Part;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import com.cloudera.oryx.api.serving.OryxServingException;
import com.cloudera.oryx.app.serving.AbstractOryxResource;
import com.cloudera.oryx.app.serving.classreg.model.ClassificationRegressionServingModel;
import com.cloudera.oryx.common.text.TextUtils;

/**
 * <p>Responds to a GET request to {@code /predict/[datum]}, or a POST to {@code /predict}
 * containing several data points, one on each line. The inputs are data points to predict,
 * delimited, like "1,foo,3.0". The value of the target feature in the input is ignored.</p>
 *
 * <p>The response body contains the result of prediction, one for each input data point, one per
 * line. The result depends on the classifier or regressor -- could be a number
 * or a category name. If JSON output is selected, the result is a JSON list.</p>
 */
@Singleton
@Path("/predict")
public final class Predict extends AbstractOryxResource {

  @GET
  @Path("{datum}")
  @Produces({MediaType.TEXT_PLAIN, "text/csv", MediaType.APPLICATION_JSON})
  public String get(@PathParam("datum") String datum) throws OryxServingException {
    return predict(datum);
  }

  @POST
  @Consumes({MediaType.TEXT_PLAIN, "text/csv", MediaType.APPLICATION_JSON})
  @Produces({MediaType.TEXT_PLAIN, "text/csv", MediaType.APPLICATION_JSON})
  public List<String> post(Reader reader) throws IOException, OryxServingException {
    return doPost(maybeBuffer(reader));
  }

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces({MediaType.TEXT_PLAIN, "text/csv", MediaType.APPLICATION_JSON})
  public List<String> post(@Context HttpServletRequest request)
      throws IOException, OryxServingException {
    List<String> result = new ArrayList<>();
    for (Part item : parseMultipart(request)) {
      try (BufferedReader reader = maybeBuffer(maybeDecompress(item))) {
        result.addAll(doPost(reader));
      }
    }
    return result;
  }

  private List<String> doPost(BufferedReader buffered) throws IOException, OryxServingException {
    List<String> predictions = new ArrayList<>();
    for (String line; (line = buffered.readLine()) != null;) {
      predictions.add(predict(line));
    }
    return predictions;
  }

  private String predict(String datum) throws OryxServingException {
    check(datum != null && !datum.isEmpty(), "Missing input data");
    ClassificationRegressionServingModel model = (ClassificationRegressionServingModel) getServingModel();
    String[] parsedDatum = TextUtils.parseDelimited(datum, ',');
    try {
      return model.predict(parsedDatum);
    } catch (IllegalArgumentException iae) {
      throw new OryxServingException(Response.Status.BAD_REQUEST, iae.getMessage());
    }
  }

}
