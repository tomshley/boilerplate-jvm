/*
 * copyright 2017–present tomshley llc
 *
 * licensed under the apache license, version 2.0 (the "license");
 * you may not use this file except in compliance with the license.
 * you may obtain a copy of the license at
 *
 * http://www.apache.org/licenses/license-2.0
 *
 * unless required by applicable law or agreed to in writing, software
 * distributed under the license is distributed on an "as is" basis,
 * without warranties or conditions of any kind, either express or implied.
 * see the license for the specific language governing permissions and
 * limitations under the license.
 *
 * @author thomas schena @sgoggles <https://github.com/sgoggles> | <https://gitlab.com/sgoggles>
 *
 */

resolvers +=
  "gitlab-maven" at "https://gitlab.com/api/v4/projects/70100400/packages/maven"

addSbtPlugin(
  "com.tomshley.magicroot" % "magicroot-sbt-projectsettings" % "2.2.0"
)

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.11")
